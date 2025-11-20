/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import org.elasticsearch.core.Tuple;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.lang.constant.ClassDesc;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;

public class AccessibleJdkMethods {

    private static final Set<AccessibleMethod.Descriptor> EXCLUDES = Set.of(
        new AccessibleMethod.Descriptor("toString", "()Ljava/lang/String;", true, false),
        new AccessibleMethod.Descriptor("hashCode", "()I", true, false),
        new AccessibleMethod.Descriptor("equals", "(Ljava/lang/Object;)Z", true, false),
        new AccessibleMethod.Descriptor("close", "()V", true, false)
    );

    public record AccessibleMethod(Descriptor descriptor, boolean isFinal, boolean isDeprecated) {
        public record Descriptor(String method, String descriptor, boolean isPublic, boolean isStatic) {
            public static final Comparator<Descriptor> COMPARATOR = Comparator.comparing(Descriptor::method)
                .thenComparing(Descriptor::descriptor)
                .thenComparing(Descriptor::isStatic);
        }

        public static final Comparator<AccessibleMethod> COMPARATOR = Comparator.comparing(
            AccessibleMethod::descriptor,
            Descriptor.COMPARATOR
        );
    }

    public record ModuleClass(String module, String clazz) {
        public static final Comparator<ModuleClass> COMPARATOR = Comparator.comparing(ModuleClass::module)
            .thenComparing(ModuleClass::clazz);
    }

    public static Stream<Tuple<ModuleClass, AccessibleMethod>> loadAccessibleMethods(Predicate<String> modulePredicate) throws IOException {
        // 1st: map class names to module names (including later excluded modules) for lookup in 2nd step
        final Map<String, String> moduleNameByClass = Utils.loadClassToModuleMapping();
        final Map<String, Set<String>> exportsByModule = Utils.loadExportsByModule();
        final AccessibleMethodsVisitor visitor = new AccessibleMethodsVisitor(modulePredicate, moduleNameByClass, exportsByModule);
        // 2nd: calculate accessible implementations of classes in included modules
        Utils.walkJdkModules(modulePredicate, exportsByModule, (moduleName, moduleClasses, moduleExports) -> {
            for (var classFile : moduleClasses) {
                // visit class once (skips if class was already visited earlier due to a dependency on it)
                visitor.visitOnce(new ModuleClass(moduleName, Utils.internalClassName(classFile, moduleName)));
            }
        });

        return visitor.getAccessibleMethods().entrySet().stream().flatMap(e -> e.getValue().stream().map(m -> Tuple.tuple(e.getKey(), m)));
    }

    private static class AccessibleMethodsVisitor extends ClassVisitor {
        private final Map<ModuleClass, Set<AccessibleMethod>> inheritableAccessByClass = new TreeMap<>(ModuleClass.COMPARATOR);
        private final Map<ModuleClass, Set<AccessibleMethod>> accessibleImplementationsByClass = new TreeMap<>(ModuleClass.COMPARATOR);

        private final Predicate<String> modulePredicate;
        private final Map<String, String> moduleNameByClass;
        private final Map<String, Set<String>> exportsByModule;

        private Set<AccessibleMethod> accessibleImplementations;
        private Set<AccessibleMethod> inheritableAccess;

        private ModuleClass moduleClass;
        private boolean isPublicClass;
        private boolean isFinalClass;
        private boolean isDeprecatedClass;
        private boolean isExported;

        AccessibleMethodsVisitor(
            Predicate<String> modulePredicate,
            Map<String, String> moduleNameByClass,
            Map<String, Set<String>> exportsByModule
        ) {
            super(Opcodes.ASM9);
            this.modulePredicate = modulePredicate;
            this.moduleNameByClass = moduleNameByClass;
            this.exportsByModule = exportsByModule;
        }

        private static Set<AccessibleMethod> newSortedSet() {
            return new TreeSet<>(AccessibleMethod.COMPARATOR);
        }

        Map<ModuleClass, Set<AccessibleMethod>> getAccessibleMethods() {
            return Collections.unmodifiableMap(accessibleImplementationsByClass);
        }

        void visitOnce(ModuleClass moduleClass) {
            if (accessibleImplementationsByClass.containsKey(moduleClass)) {
                return;
            }
            if (moduleClass.clazz.startsWith("com/sun/") && moduleClass.clazz.contains("/internal/")) {
                // skip com.sun.*.internal classes as they are not part of the supported JDK API
                // even if methods override some publicly visible API
                return;
            }
            try {
                ClassReader cr = new ClassReader(moduleClass.clazz);
                cr.accept(this, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            final Set<AccessibleMethod> currentInheritedAccess = newSortedSet();
            if (superName != null) {
                var superModuleClass = getModuleClassFromName(superName);
                visitOnce(superModuleClass);
                currentInheritedAccess.addAll(inheritableAccessByClass.getOrDefault(superModuleClass, emptySet()));
            }
            if (interfaces != null && interfaces.length > 0) {
                for (var interfaceName : interfaces) {
                    var interfaceModuleClass = getModuleClassFromName(interfaceName);
                    visitOnce(interfaceModuleClass);
                    currentInheritedAccess.addAll(inheritableAccessByClass.getOrDefault(interfaceModuleClass, emptySet()));
                }
            }
            // only initialize local state AFTER visiting all dependencies above!
            super.visit(version, access, name, signature, superName, interfaces);
            this.moduleClass = getModuleClassFromName(name);
            this.isExported = getModuleExports(moduleClass.module()).contains(getPackageName(name));
            this.isPublicClass = (access & Opcodes.ACC_PUBLIC) != 0;
            this.isFinalClass = (access & Opcodes.ACC_FINAL) != 0;
            this.isDeprecatedClass = (access & Opcodes.ACC_DEPRECATED) != 0;
            this.inheritableAccess = currentInheritedAccess;
            this.accessibleImplementations = newSortedSet();
        }

        private ModuleClass getModuleClassFromName(String name) {
            String module = moduleNameByClass.get(name);
            if (module == null) {
                throw new IllegalStateException("Unknown module for class: " + name);
            }
            return new ModuleClass(module, name);
        }

        private Set<String> getModuleExports(String module) {
            Set<String> exports = exportsByModule.get(module);
            if (exports == null) {
                throw new IllegalStateException("Unknown exports for module: " + module);
            }
            return exports;
        }

        @Override
        public void visitEnd() {
            super.visitEnd();
            if (accessibleImplementationsByClass.put(moduleClass, unmodifiableSet(accessibleImplementations)) != null
                || inheritableAccessByClass.put(moduleClass, unmodifiableSet(inheritableAccess)) != null) {
                throw new IllegalStateException("Class " + moduleClass.clazz() + " was already visited!");
            }
        }

        private static Set<AccessibleMethod> unmodifiableSet(Set<AccessibleMethod> set) {
            return set.isEmpty() ? emptySet() : Collections.unmodifiableSet(set);
        }

        private static String getPackageName(String className) {
            return ClassDesc.ofInternalName(className).packageName();
        }

        @Override
        public final MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            boolean isPublic = (access & Opcodes.ACC_PUBLIC) != 0;
            boolean isProtected = (access & Opcodes.ACC_PROTECTED) != 0;
            boolean isFinal = (access & Opcodes.ACC_FINAL) != 0;
            boolean isStatic = (access & Opcodes.ACC_STATIC) != 0;
            boolean isDeprecated = (access & Opcodes.ACC_DEPRECATED) != 0;
            if ((isPublic || isProtected) == false) {
                return mv;
            }

            var methodDescriptor = new AccessibleMethod.Descriptor(name, descriptor, isPublic, isStatic);
            var method = new AccessibleMethod(methodDescriptor, isFinal, isDeprecatedClass || isDeprecated);
            if (isPublicClass && isExported && EXCLUDES.contains(methodDescriptor) == false) {
                // class is public and exported, to be accessible outside the JDK the method must be either:
                // - public or
                // - protected if not a final class
                if (isPublic || isFinalClass == false) {
                    if (modulePredicate.test(moduleClass.module)) {
                        accessibleImplementations.add(method);
                    }
                    // if public and not static, the method can be accessible on non-public and non-exported subclasses,
                    // but skip constructors
                    if (isPublic && isStatic == false && name.equals("<init>") == false) {
                        inheritableAccess.add(method);
                    }
                }
            } else if (inheritableAccess.contains(method)) {
                if (modulePredicate.test(moduleClass.module)) {
                    accessibleImplementations.add(method);
                }
            }
            return mv;
        }
    }
}
