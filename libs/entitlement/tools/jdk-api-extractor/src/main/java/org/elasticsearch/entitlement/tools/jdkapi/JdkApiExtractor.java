/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.jdkapi;

import org.elasticsearch.entitlement.tools.Utils;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;

import java.io.IOException;
import java.lang.constant.ClassDesc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

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
        if (args.length != 1) {
            System.err.println("Exactly one argument is required: the output file path");
            System.exit(1);
        }

        Map<String, Set<AccessibleMethod>> accessibleImplementationsByClass = new TreeMap<>();
        Map<String, Set<AccessibleMethod>> accessibleForOverwritesByClass = new TreeMap<>();

        Utils.walkJdkModules((moduleName, moduleClasses, moduleExports) -> {
            var visitor = new AccessibleClassVisitor(moduleExports, accessibleImplementationsByClass, accessibleForOverwritesByClass);
            for (var classFile : moduleClasses) {
                try {
                    ClassReader cr = new ClassReader(Files.newInputStream(classFile));
                    cr.accept(visitor, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Path path = Path.of(args[0]);
        System.out.println("Writing accessible methods of " + Runtime.version() + " to " + path.toAbsolutePath());

        Files.write(
            path,
            () -> accessibleImplementationsByClass.entrySet().stream().flatMap(AccessibleMethod::toLines).iterator(),
            StandardCharsets.UTF_8
        );
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
        private final Map<String, Set<AccessibleMethod>> accessibleForOverwritesByClass;

        private Set<AccessibleMethod> accessibleImplementations;
        private Set<AccessibleMethod> accessibleForOverwrites;

        private String className;
        private boolean isPublicClass;
        private boolean isFinalClass;
        private boolean isExported;

        AccessibleClassVisitor(
            Set<String> moduleExports,
            Map<String, Set<AccessibleMethod>> accessibleImplementationsByClass,
            Map<String, Set<AccessibleMethod>> accessibleForOverwritesByClass
        ) {
            super(ASM9);
            this.moduleExports = moduleExports;
            this.accessibleImplementationsByClass = accessibleImplementationsByClass;
            this.accessibleForOverwritesByClass = accessibleForOverwritesByClass;
        }

        private Set<AccessibleMethod> getMethods(Map<String, Set<AccessibleMethod>> methods, String clazz) {
            return methods.computeIfAbsent(clazz, k -> new TreeSet<>(AccessibleMethod.COMPARATOR));
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            if (superName != null) {
                visitSuperClass(superName);
                getMethods(accessibleForOverwritesByClass, name).addAll(getMethods(accessibleForOverwritesByClass, superName));
            }
            if (interfaces != null && interfaces.length > 0) {
                for (var interfaceName : interfaces) {
                    visitInterface(interfaceName);
                    getMethods(accessibleForOverwritesByClass, name).addAll(getMethods(accessibleForOverwritesByClass, interfaceName));
                }
            }
            super.visit(version, access, name, signature, superName, interfaces);
            this.isExported = moduleExports.contains(getPackageName(name));
            this.className = name;
            this.isPublicClass = (access & ACC_PUBLIC) != 0;
            this.isFinalClass = (access & ACC_FINAL) != 0;
            this.accessibleImplementations = getMethods(accessibleImplementationsByClass, name);
            this.accessibleForOverwrites = getMethods(accessibleForOverwritesByClass, name);
        }

        @Override
        public void visitEnd() {
            super.visitEnd();
            if (accessibleImplementations.isEmpty()) {
                accessibleImplementationsByClass.remove(className);
            }
            if (accessibleForOverwrites.isEmpty()) {
                accessibleForOverwritesByClass.remove(className);
            }
        }

        private void visitSuperClass(String superName) {
            try {
                ClassReader cr = new ClassReader(superName);
                cr.accept(this, 0);
            } catch (IOException e) {
                System.out.println("Failed to visit super class [" + superName + "]:" + e.getMessage());
            }
        }

        private void visitInterface(String interfaceName) {
            try {
                ClassReader cr = new ClassReader(interfaceName);
                cr.accept(this, 0);
            } catch (IOException e) {
                System.out.println("Failed to visit interface [" + interfaceName + "]:" + e.getMessage());

            }
        }

        public String getClassName() {
            return className;
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
            if ((isPublic || isProtected) == false) {
                return mv;
            }

            var method = new AccessibleMethod(name, descriptor, isPublic, isFinal, isStatic);
            if (isPublicClass && isExported && EXCLUDES.contains(method) == false) {
                // class is public and exported, for final classes skip non-public methods
                if (isPublic || isFinalClass == false) {
                    accessibleImplementations.add(method);
                }
                // if not static, the method is accessible for overwrites
                if (isStatic == false) {
                    accessibleForOverwrites.add(method);
                }
            } else if (accessibleForOverwrites.contains(method)) {
                accessibleImplementations.add(method);
            }
            return mv;
        }
    }
}
