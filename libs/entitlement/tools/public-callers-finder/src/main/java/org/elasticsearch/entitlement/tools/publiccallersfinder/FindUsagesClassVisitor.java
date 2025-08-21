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
import org.objectweb.asm.Type;

import java.lang.constant.ClassDesc;
import java.lang.reflect.AccessFlag;
import java.util.EnumSet;
import java.util.Set;

import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ASM9;

class FindUsagesClassVisitor extends ClassVisitor {

    private int classAccess;
    private boolean accessibleViaInterfaces;

    record MethodDescriptor(String className, String methodName, String methodDescriptor) {}

    record EntryPoint(
        String moduleName,
        String source,
        int line,
        String className,
        String methodName,
        String methodDescriptor,
        EnumSet<ExternalAccess> access
    ) {}

    interface CallerConsumer {
        void accept(String source, int line, String className, String methodName, String methodDescriptor, EnumSet<ExternalAccess> access);
    }

    private final Set<String> moduleExports;
    private final MethodDescriptor methodToFind;
    private final CallerConsumer callers;
    private String className;
    private String source;

    protected FindUsagesClassVisitor(Set<String> moduleExports, MethodDescriptor methodToFind, CallerConsumer callers) {
        super(ASM9);
        this.moduleExports = moduleExports;
        this.methodToFind = methodToFind;
        this.callers = callers;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.className = name;
        this.classAccess = access;
        if (interfaces.length > 0) {
            this.accessibleViaInterfaces = findAccessibility(interfaces, moduleExports);
        }
    }

    private static boolean findAccessibility(String[] interfaces, Set<String> moduleExports) {
        var accessibleViaInterfaces = false;
        for (var interfaceName : interfaces) {
            if (moduleExports.contains(getPackageName(interfaceName))) {
                var interfaceType = Type.getObjectType(interfaceName);
                try {
                    var clazz = Class.forName(interfaceType.getClassName());
                    if (clazz.accessFlags().contains(AccessFlag.PUBLIC)) {
                        accessibleViaInterfaces = true;
                    }
                } catch (ClassNotFoundException ignored) {}
            }
        }
        return accessibleViaInterfaces;
    }

    @Override
    public void visitSource(String source, String debug) {
        super.visitSource(source, debug);
        this.source = source;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        return new FindUsagesMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions), name, descriptor, access);
    }

    private static String getPackageName(String className) {
        return ClassDesc.ofInternalName(className).packageName();
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

            if (methodToFind.className.equals(owner)) {
                if (methodToFind.methodName.equals(name)) {
                    if (methodToFind.methodDescriptor == null || methodToFind.methodDescriptor.equals(descriptor)) {
                        EnumSet<ExternalAccess> externalAccess = ExternalAccess.fromPermissions(
                            moduleExports.contains(getPackageName(className)),
                            accessibleViaInterfaces || (classAccess & ACC_PUBLIC) != 0,
                            (methodAccess & ACC_PUBLIC) != 0,
                            (methodAccess & ACC_PROTECTED) != 0
                        );
                        callers.accept(source, line, className, methodName, methodDescriptor, externalAccess);
                    }
                }
            }
        }

        @Override
        public void visitLineNumber(int line, Label start) {
            super.visitLineNumber(line, start);
            this.line = line;
        }
    }
}
