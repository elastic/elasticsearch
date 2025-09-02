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
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

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

    public static void main(String[] args) throws IOException {
        validateArgs(args);
        boolean deprecationsOnly = args.length == 2 && args[1].equals("--deprecations-only");

        Map<String, Set<AccessibleMethod>> accessibleImplementationsByClass = new TreeMap<>();
        Map<String, Set<AccessibleMethod>> accessibleForOverridesByClass = new TreeMap<>();
        Map<String, Set<AccessibleMethod>> deprecationsByClass = new TreeMap<>();

        Utils.walkJdkModules((moduleName, moduleClasses, moduleExports) -> {
            var visitor = new AccessibleClassVisitor(
                moduleExports,
                accessibleImplementationsByClass,
                accessibleForOverridesByClass,
                deprecationsByClass
            );
            for (var classFile : moduleClasses) {
                // skip if class was already visited earlier due to a dependency on it
                if (accessibleImplementationsByClass.containsKey(internalClassName(classFile, moduleName))) {
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

        writeFile(Path.of(args[0]), deprecationsOnly ? deprecationsByClass : accessibleImplementationsByClass);
    }

    private static String internalClassName(Path clazz, String moduleName) {
        Path modulePath = clazz.getFileSystem().getPath("modules", moduleName);
        String relativePath = modulePath.relativize(clazz).toString();
        return relativePath.substring(0, relativePath.length() - ".class".length());
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void validateArgs(String[] args) {
        boolean valid = args.length == 1 || (args.length == 2 && "--deprecations-only".equals(args[1]));

        if (valid == false) {
            System.err.println("usage: <output file path> [--deprecations-only]");
            System.exit(1);
        }
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void writeFile(Path path, Map<String, Set<AccessibleMethod>> methods) throws IOException {
        System.out.println("Writing result for " + Runtime.version() + " to " + path.toAbsolutePath());
        Files.write(path, () -> methods.entrySet().stream().flatMap(AccessibleMethod::toLines).iterator(), StandardCharsets.UTF_8);
    }

    record AccessibleMethod(String method, String descriptor, boolean isPublic, boolean isFinal, boolean isStatic) {

        private static final String SEPARATOR = "\t";

        private static final Comparator<AccessibleMethod> COMPARATOR = Comparator.comparing(AccessibleMethod::method)
            .thenComparing(AccessibleMethod::descriptor)
            .thenComparing(AccessibleMethod::isStatic);

        CharSequence toLine(String clazz) {
            return String.join(
                SEPARATOR,
                clazz,
                method,
                descriptor,
                isPublic ? "PUBLIC" : "PROTECTED",
                isStatic ? "STATIC" : "",
                isFinal ? "FINAL" : ""
            );
        }

        static Stream<CharSequence> toLines(Map.Entry<String, Set<AccessibleMethod>> entry) {
            return entry.getValue().stream().map(m -> m.toLine(entry.getKey()));
        }
    }

    static class AccessibleClassVisitor extends ClassVisitor {
        private final Set<String> moduleExports;
        private final Map<String, Set<AccessibleMethod>> accessibleImplementationsByClass;
        private final Map<String, Set<AccessibleMethod>> accessibleForOverridesByClass;
        private final Map<String, Set<AccessibleMethod>> deprecationsByClass;

        private Set<AccessibleMethod> accessibleImplementations;
        private Set<AccessibleMethod> accessibleForOverrides;
        private Set<AccessibleMethod> deprecations;

        private String className;
        private boolean isPublicClass;
        private boolean isFinalClass;
        private boolean isDeprecatedClass;
        private boolean isExported;

        AccessibleClassVisitor(
            Set<String> moduleExports,
            Map<String, Set<AccessibleMethod>> accessibleImplementationsByClass,
            Map<String, Set<AccessibleMethod>> accessibleForOverridesByClass,
            Map<String, Set<AccessibleMethod>> deprecationsByClass
        ) {
            super(ASM9);
            this.moduleExports = moduleExports;
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
                if (accessibleImplementationsByClass.containsKey(superName) == false) {
                    visitSuperClass(superName);
                }
                currentAccessibleForOverrides.addAll(accessibleForOverridesByClass.getOrDefault(superName, Collections.emptySet()));
            }
            if (interfaces != null && interfaces.length > 0) {
                for (var interfaceName : interfaces) {
                    if (accessibleImplementationsByClass.containsKey(interfaceName) == false) {
                        visitInterface(interfaceName);
                    }
                    currentAccessibleForOverrides.addAll(accessibleForOverridesByClass.getOrDefault(interfaceName, Collections.emptySet()));
                }
            }
            // only initialize local state AFTER visiting all dependencies above!
            super.visit(version, access, name, signature, superName, interfaces);
            this.isExported = moduleExports.contains(getPackageName(name));
            this.className = name;
            this.isPublicClass = (access & ACC_PUBLIC) != 0;
            this.isFinalClass = (access & ACC_FINAL) != 0;
            this.isDeprecatedClass = (access & ACC_DEPRECATED) != 0;
            this.accessibleForOverrides = currentAccessibleForOverrides;
            this.accessibleImplementations = newSortedSet();
            this.deprecations = newSortedSet();
        }

        @Override
        public void visitEnd() {
            super.visitEnd();
            if (accessibleImplementationsByClass.put(className, unmodifiableSet(accessibleImplementations)) != null
                || accessibleForOverridesByClass.put(className, unmodifiableSet(accessibleForOverrides)) != null
                || deprecationsByClass.put(className, unmodifiableSet(deprecations)) != null) {
                throw new IllegalStateException("Class " + className + " was already visited!");
            }
        }

        private static Set<AccessibleMethod> unmodifiableSet(Set<AccessibleMethod> set) {
            return set.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(set);
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
