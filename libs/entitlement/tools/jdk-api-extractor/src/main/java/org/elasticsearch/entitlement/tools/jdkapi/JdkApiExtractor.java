/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.jdkapi;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.tools.Utils;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;

import java.io.IOException;
import java.lang.constant.ClassDesc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.objectweb.asm.Opcodes.ACC_DEPRECATED;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ASM9;

public class JdkApiExtractor {

    // exclude both final and non-final variants of these
    private static final Set<AccessibleMethod> EXCLUDES = Set.of(
        new AccessibleMethod("toString", "()Ljava/lang/String;", true, false, false),
        new AccessibleMethod("hashCode", "()I", true, false, false),
        new AccessibleMethod("equals", "(Ljava/lang/Object;)Z", true, false, false),
        new AccessibleMethod("close", "()V", true, false, false),
        new AccessibleMethod("toString", "()Ljava/lang/String;", true, true, false),
        new AccessibleMethod("hashCode", "()I", true, false, true),
        new AccessibleMethod("equals", "(Ljava/lang/Object;)Z", true, true, false),
        new AccessibleMethod("close", "()V", true, false, true)
    );

    private static String DEPRECATIONS_ONLY = "--deprecations-only";
    private static String INCLUDE_INCUBATOR = "--include-incubator";

    private static Set<String> OPTIONAL_ARGS = Set.of(DEPRECATIONS_ONLY, INCLUDE_INCUBATOR);

    public static void main(String[] args) throws IOException {
        validateArgs(args);
        boolean deprecationsOnly = optionalArgs(args).anyMatch(DEPRECATIONS_ONLY::equals);

        final Map<String, String> moduleNameByClass = new HashMap<>();
        final Map<ModuleClass, Set<AccessibleMethod>> accessibleImplementationsByClass = new TreeMap<>(ModuleClass.COMPARATOR);
        final Map<ModuleClass, Set<AccessibleMethod>> accessibleForOverridesByClass = new TreeMap<>(ModuleClass.COMPARATOR);
        final Map<ModuleClass, Set<AccessibleMethod>> deprecationsByClass = new TreeMap<>(ModuleClass.COMPARATOR);

        final Map<String, Set<String>> exportsByModule = Utils.findModuleExports();
        // 1st: map class names to module names (including later excluded modules) for lookup in 2nd step
        Utils.walkJdkModules(m -> true, exportsByModule, (moduleName, moduleClasses, moduleExports) -> {
            for (var classFile : moduleClasses) {
                String prev = moduleNameByClass.put(internalClassName(classFile, moduleName), moduleName);
                if (prev != null) {
                    throw new IllegalStateException("Class " + classFile + " is in both modules " + prev + " and " + moduleName);
                }
            }
        });

        var visitor = new AccessibleClassVisitor(
            moduleNameByClass,
            exportsByModule,
            accessibleImplementationsByClass,
            accessibleForOverridesByClass,
            deprecationsByClass
        );
        Predicate<String> modulePredicate = Utils.DEFAULT_MODULE_PREDICATE.or(
            m -> optionalArgs(args).anyMatch(INCLUDE_INCUBATOR::equals) && m.contains(".incubator.")
        );
        // 2nd: calculate accessible implementations of classes in included modules
        Utils.walkJdkModules(modulePredicate, exportsByModule, (moduleName, moduleClasses, moduleExports) -> {
            for (var classFile : moduleClasses) {
                // skip if class was already visited earlier due to a dependency on it
                String className = internalClassName(classFile, moduleName);
                if (accessibleImplementationsByClass.containsKey(new ModuleClass(moduleName, className))) {
                    continue;
                }
                try {
                    ClassReader cr = new ClassReader(Files.newInputStream(classFile));
                    cr.accept(visitor, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // finally, skip some implementations we're not interested in
        Predicate<Map.Entry<ModuleClass, Set<AccessibleMethod>>> predicate = entry -> {
            if (entry.getKey().clazz.startsWith("com/sun/") && entry.getKey().clazz.contains("/internal/")) {
                // skip com.sun.*.internal classes as they are not part of the supported JDK API
                // even if methods override some publicly visible API
                return false;
            }
            // skip classes that are not part of included modules, but checked due to dependencies
            return modulePredicate.test(entry.getKey().module);
        };
        writeFile(Path.of(args[0]), deprecationsOnly ? deprecationsByClass : accessibleImplementationsByClass, predicate);
    }

    private static String internalClassName(Path clazz, String moduleName) {
        Path modulePath = clazz.getFileSystem().getPath("modules", moduleName);
        String relativePath = modulePath.relativize(clazz).toString();
        return relativePath.substring(0, relativePath.length() - ".class".length());
    }

    private static Stream<String> optionalArgs(String[] args) {
        return Arrays.stream(args).skip(1);
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void validateArgs(String[] args) {
        boolean valid = args.length > 0 && optionalArgs(args).allMatch(OPTIONAL_ARGS::contains);
        if (valid && isWritableOutputPath(args[0]) == false) {
            valid = false;
            System.err.println("invalid output path: " + args[0]);
        }
        if (valid == false) {
            String optionalArgs = OPTIONAL_ARGS.stream().collect(Collectors.joining("] [", " [", "]"));
            System.err.println("usage: <output file path>" + optionalArgs);
            System.exit(1);
        }
    }

    private static boolean isWritableOutputPath(String pathStr) {
        try {
            Path path = Paths.get(pathStr);
            if (Files.exists(path) && Files.isRegularFile(path)) {
                return Files.isWritable(path);
            }
            Path parent = path.toAbsolutePath().getParent();
            return parent != null && Files.isDirectory(parent) && Files.isWritable(parent);
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void writeFile(
        Path path,
        Map<ModuleClass, Set<AccessibleMethod>> methods,
        Predicate<Map.Entry<ModuleClass, Set<AccessibleMethod>>> predicate
    ) throws IOException {
        System.out.println("Writing result for " + Runtime.version() + " to " + path.toAbsolutePath());
        Files.write(
            path,
            () -> methods.entrySet().stream().filter(predicate).flatMap(AccessibleMethod::toLines).iterator(),
            StandardCharsets.UTF_8
        );
    }

    record ModuleClass(String module, String clazz) {
        private static final Comparator<ModuleClass> COMPARATOR = Comparator.comparing(ModuleClass::module)
            .thenComparing(ModuleClass::clazz);
    }

    record AccessibleMethod(String method, String descriptor, boolean isPublic, boolean isFinal, boolean isStatic) {

        private static final String SEPARATOR = "\t";

        private static final Comparator<AccessibleMethod> COMPARATOR = Comparator.comparing(AccessibleMethod::method)
            .thenComparing(AccessibleMethod::descriptor)
            .thenComparing(AccessibleMethod::isStatic);

        CharSequence toLine(ModuleClass moduleClass) {
            return String.join(
                SEPARATOR,
                moduleClass.module,
                moduleClass.clazz,
                method,
                descriptor,
                isPublic ? "PUBLIC" : "PROTECTED",
                isStatic ? "STATIC" : "",
                isFinal ? "FINAL" : ""
            );
        }

        static Stream<CharSequence> toLines(Map.Entry<ModuleClass, Set<AccessibleMethod>> entry) {
            return entry.getValue().stream().map(m -> m.toLine(entry.getKey()));
        }
    }

    static class AccessibleClassVisitor extends ClassVisitor {
        private final Map<String, String> moduleNameByClass;
        private final Map<String, Set<String>> exportsByModule;
        private final Map<ModuleClass, Set<AccessibleMethod>> accessibleImplementationsByClass;
        private final Map<ModuleClass, Set<AccessibleMethod>> accessibleForOverridesByClass;
        private final Map<ModuleClass, Set<AccessibleMethod>> deprecationsByClass;

        private Set<AccessibleMethod> accessibleImplementations;
        private Set<AccessibleMethod> accessibleForOverrides;
        private Set<AccessibleMethod> deprecations;

        private ModuleClass moduleClass;
        private boolean isPublicClass;
        private boolean isFinalClass;
        private boolean isDeprecatedClass;
        private boolean isExported;

        AccessibleClassVisitor(
            Map<String, String> moduleNameByClass,
            Map<String, Set<String>> exportsByModule,
            Map<ModuleClass, Set<AccessibleMethod>> accessibleImplementationsByClass,
            Map<ModuleClass, Set<AccessibleMethod>> accessibleForOverridesByClass,
            Map<ModuleClass, Set<AccessibleMethod>> deprecationsByClass
        ) {
            super(ASM9);
            this.moduleNameByClass = moduleNameByClass;
            this.exportsByModule = exportsByModule;
            this.accessibleImplementationsByClass = accessibleImplementationsByClass;
            this.accessibleForOverridesByClass = accessibleForOverridesByClass;
            this.deprecationsByClass = deprecationsByClass;
        }

        private static Set<AccessibleMethod> newSortedSet() {
            return new TreeSet<>(AccessibleMethod.COMPARATOR);
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            final Set<AccessibleMethod> currentAccessibleForOverrides = newSortedSet();
            if (superName != null) {
                var superModuleClass = getModuleClass(superName);
                if (accessibleImplementationsByClass.containsKey(superModuleClass) == false) {
                    visitSuperClass(superName);
                }
                currentAccessibleForOverrides.addAll(accessibleForOverridesByClass.getOrDefault(superModuleClass, emptySet()));
            }
            if (interfaces != null && interfaces.length > 0) {
                for (var interfaceName : interfaces) {
                    var interfaceModuleClass = getModuleClass(interfaceName);
                    if (accessibleImplementationsByClass.containsKey(interfaceModuleClass) == false) {
                        visitInterface(interfaceName);
                    }
                    currentAccessibleForOverrides.addAll(accessibleForOverridesByClass.getOrDefault(interfaceModuleClass, emptySet()));
                }
            }
            // only initialize local state AFTER visiting all dependencies above!
            super.visit(version, access, name, signature, superName, interfaces);
            this.moduleClass = getModuleClass(name);
            this.isExported = getModuleExports(moduleClass.module).contains(getPackageName(name));
            this.isPublicClass = (access & ACC_PUBLIC) != 0;
            this.isFinalClass = (access & ACC_FINAL) != 0;
            this.isDeprecatedClass = (access & ACC_DEPRECATED) != 0;
            this.accessibleForOverrides = currentAccessibleForOverrides;
            this.accessibleImplementations = newSortedSet();
            this.deprecations = newSortedSet();
        }

        private ModuleClass getModuleClass(String name) {
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
                || accessibleForOverridesByClass.put(moduleClass, unmodifiableSet(accessibleForOverrides)) != null
                || deprecationsByClass.put(moduleClass, unmodifiableSet(deprecations)) != null) {
                throw new IllegalStateException("Class " + moduleClass.clazz + " was already visited!");
            }
        }

        private static Set<AccessibleMethod> unmodifiableSet(Set<AccessibleMethod> set) {
            return set.isEmpty() ? emptySet() : Collections.unmodifiableSet(set);
        }

        @SuppressForbidden(reason = "cli tool printing to standard err/out")
        private void visitSuperClass(String superName) {
            try {
                ClassReader cr = new ClassReader(superName);
                cr.accept(this, 0);
            } catch (IOException e) {
                System.out.println("Failed to visit super class [" + superName + "]:" + e.getMessage());
            }
        }

        @SuppressForbidden(reason = "cli tool printing to standard err/out")
        private void visitInterface(String interfaceName) {
            try {
                ClassReader cr = new ClassReader(interfaceName);
                cr.accept(this, 0);
            } catch (IOException e) {
                System.out.println("Failed to visit interface [" + interfaceName + "]:" + e.getMessage());
            }
        }

        private static String getPackageName(String className) {
            return ClassDesc.ofInternalName(className).packageName();
        }

        @Override
        public final MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            boolean isPublic = (access & ACC_PUBLIC) != 0;
            boolean isProtected = (access & ACC_PROTECTED) != 0;
            boolean isFinal = (access & ACC_FINAL) != 0;
            boolean isStatic = (access & ACC_STATIC) != 0;
            boolean isDeprecated = (access & ACC_DEPRECATED) != 0;
            if ((isPublic || isProtected) == false) {
                return mv;
            }

            var method = new AccessibleMethod(name, descriptor, isPublic, isFinal, isStatic);
            if (isPublicClass && isExported && EXCLUDES.contains(method) == false) {
                // class is public and exported, for final classes skip non-public methods
                if (isPublic || isFinalClass == false) {
                    accessibleImplementations.add(method);
                    // if not static, the method is accessible for overrides
                    if (isStatic == false) {
                        accessibleForOverrides.add(method);
                    }
                    if (isDeprecatedClass || isDeprecated) {
                        deprecations.add(method);
                    }
                }
            } else if (accessibleForOverrides.contains(method)) {
                accessibleImplementations.add(method);
                if (isDeprecatedClass || isDeprecated) {
                    deprecations.add(method);
                }
            }
            return mv;
        }
    }
}
