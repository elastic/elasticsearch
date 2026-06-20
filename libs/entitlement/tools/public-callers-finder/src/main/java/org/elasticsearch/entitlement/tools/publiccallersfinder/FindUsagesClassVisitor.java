/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.emptySet;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ASM9;

class FindUsagesClassVisitor extends ClassVisitor {

    /**
     * Internal classes that should be skipped for further transitive analysis.
     */
    private static Map<MethodDescriptor, Set<String>> SKIPS = Map.of(
        // heavily used internal low-level APIs used to write bytecode by MethodHandles and similar
        new MethodDescriptor("java/nio/file/Files", "write", "(Ljava/nio/file/Path;[B[Ljava/nio/file/OpenOption;)Ljava/nio/file/Path;"),
        Set.of("jdk/internal/foreign/abi/BindingSpecializer", "jdk/internal/util/ClassFileDumper")
    );

    record MethodDescriptor(String className, String methodName, String methodDescriptor) {}

    record EntryPoint(String moduleName, String source, int line, MethodDescriptor method, EnumSet<ExternalAccess> access) {}

    interface CallerConsumer {
        void accept(String source, int line, MethodDescriptor method, EnumSet<ExternalAccess> access);
    }

    private final MethodDescriptor methodToFind;
    private final Predicate<MethodDescriptor> isPublicAccessible;
    private final CallerConsumer callers;
    private String className;
    private String source;

    protected FindUsagesClassVisitor(
        MethodDescriptor methodToFind,
        Predicate<MethodDescriptor> isPublicAccessible,
        CallerConsumer callers
    ) {
        super(ASM9);
        this.methodToFind = methodToFind;
        this.isPublicAccessible = isPublicAccessible;
        this.callers = callers;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.className = name;
    }

    @Override
    public void visitSource(String source, String debug) {
        super.visitSource(source, debug);
        this.source = source;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        if (SKIPS.getOrDefault(methodToFind, emptySet()).contains(className)) {
            return null;
        }
        return new FindUsagesMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions), name, descriptor, access);
    }

    private class FindUsagesMethodVisitor extends MethodVisitor {

        private final String methodName;
        private int line;
        private final String methodDescriptor;
        private final int methodAccess;

        protected FindUsagesMethodVisitor(MethodVisitor mv, String methodName, String methodDescriptor, int methodAccess) {
            super(ASM9, mv);
            this.methodName = methodName;
            this.methodDescriptor = methodDescriptor;
            this.methodAccess = methodAccess;
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);

            if (methodToFind.className.equals(owner)
                && methodToFind.methodName.equals(name)
                && (methodToFind.methodDescriptor == null || methodToFind.methodDescriptor.equals(descriptor))) {

                MethodDescriptor method = new MethodDescriptor(className, methodName, methodDescriptor);
                EnumSet<ExternalAccess> externalAccess = ExternalAccess.fromPermissions(
                    isPublicAccessible.test(method),
                    (methodAccess & ACC_PUBLIC) != 0,
                    (methodAccess & ACC_PROTECTED) != 0
                );
                callers.accept(source, line, method, externalAccess);
            }
        }

        @Override
        public void visitLineNumber(int line, Label start) {
            super.visitLineNumber(line, start);
            this.line = line;
        }
    }
}
