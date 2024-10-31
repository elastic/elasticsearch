/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.securitymanager.scanner;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

import java.lang.constant.ClassDesc;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.INVOKEDYNAMIC;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.NEW;

class SecurityCheckClassVisitor extends ClassVisitor {

    static final String SECURITY_MANAGER_INTERNAL_NAME = "java/lang/SecurityManager";
    static final Set<String> excludedClasses = Set.of(SECURITY_MANAGER_INTERNAL_NAME);

    record CallerInfo(
        String moduleName,
        String source,
        int line,
        String className,
        String methodName,
        boolean isPublic,
        String permissionType,
        String runtimePermissionType
    ) {}

    private final Map<String, List<CallerInfo>> callerInfoByMethod;
    private String className;
    private int classAccess;
    private String source;
    private String moduleName;
    private String sourcePath;
    private Set<String> moduleExports;

    protected SecurityCheckClassVisitor(Map<String, List<CallerInfo>> callerInfoByMethod) {
        super(ASM9);
        this.callerInfoByMethod = callerInfoByMethod;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.className = name;
        this.classAccess = access;
    }

    @Override
    public void visitSource(String source, String debug) {
        super.visitSource(source, debug);
        this.source = source;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        if (excludedClasses.contains(this.className)) {
            return super.visitMethod(access, name, descriptor, signature, exceptions);
        }
        return new SecurityCheckMethodVisitor(
            new TraceMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions), new Textifier()),
            name,
            access
        );
    }

    public void setCurrentModule(String moduleName, Set<String> moduleExports) {
        this.moduleName = moduleName;
        this.moduleExports = moduleExports;
    }

    public void setCurrentSourcePath(String path) {
        this.sourcePath = path;
    }

    private class SecurityCheckMethodVisitor extends MethodVisitor {

        private final String methodName;
        private int line;
        private boolean callsTarget;
        private final TraceMethodVisitor traceMethodVisitor;
        private String permissionType;
        private String runtimePermissionType;
        private final int methodAccess;

        protected SecurityCheckMethodVisitor(TraceMethodVisitor mv, String methodName, int methodAccess) {
            super(ASM9, mv);
            this.methodName = methodName;
            this.traceMethodVisitor = mv;
            this.methodAccess = methodAccess;
        }

        private static final Set<String> KNOWN_PERMISSIONS = Set.of("jdk.vm.ci.services.JVMCIPermission");

        @Override
        public void visitTypeInsn(int opcode, String type) {
            super.visitTypeInsn(opcode, type);
            if (opcode == NEW) {
                if (type.endsWith("Permission")) {
                    var objectType = Type.getObjectType(type);
                    if (KNOWN_PERMISSIONS.contains(objectType.getClassName())) {
                        permissionType = type;
                    } else {
                        try {
                            var clazz = Class.forName(objectType.getClassName());
                            if (Permission.class.isAssignableFrom(clazz)) {
                                permissionType = type;
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
            super.visitFieldInsn(opcode, owner, name, descriptor);
            if (opcode == GETSTATIC && descriptor.endsWith("Permission;")) {
                var permissionType = Type.getType(descriptor);
                if (permissionType.getSort() == Type.ARRAY) {
                    permissionType = permissionType.getElementType();
                }
                try {
                    var clazz = Class.forName(permissionType.getClassName());
                    if (Permission.class.isAssignableFrom(clazz)) {
                        this.permissionType = permissionType.getInternalName();
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                var objectType = Type.getObjectType(owner);
                try {
                    var clazz = Class.forName(objectType.getClassName());
                    Arrays.stream(clazz.getDeclaredFields())
                        .filter(f -> Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers()))
                        .filter(f -> f.getName().equals(name))
                        .findFirst()
                        .ifPresent(x -> {
                            if (Permission.class.isAssignableFrom(x.getType())) {
                                try {
                                    x.setAccessible(true);
                                    var p = (Permission) (x.get(null));
                                    this.runtimePermissionType = p.getName();
                                } catch (IllegalAccessException | InaccessibleObjectException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

                } catch (ClassNotFoundException | NoClassDefFoundError | UnsatisfiedLinkError e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void visitLdcInsn(Object value) {
            super.visitLdcInsn(value);
            if (permissionType != null && permissionType.equals("java/lang/RuntimePermission")) {
                this.runtimePermissionType = value.toString();
            }
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            if (opcode == INVOKEVIRTUAL
                || opcode == INVOKESPECIAL
                || opcode == INVOKESTATIC
                || opcode == INVOKEINTERFACE
                || opcode == INVOKEDYNAMIC) {

                if (SECURITY_MANAGER_INTERNAL_NAME.equals(owner)) {
                    boolean callerIsPublic = (methodAccess & ACC_PUBLIC) != 0
                        && (classAccess & ACC_PUBLIC) != 0
                        && moduleExports.contains(getPackageName(className));

                    if (name.equals("checkPermission")) {
                        this.callsTarget = true;
                        var callers = callerInfoByMethod.computeIfAbsent(name, ignored -> new ArrayList<>());
                        callers.add(
                            new CallerInfo(
                                moduleName,
                                Path.of(sourcePath, source).toString(),
                                line,
                                className,
                                methodName,
                                callerIsPublic,
                                permissionType,
                                runtimePermissionType
                            )
                        );
                        this.permissionType = null;
                        this.runtimePermissionType = null;
                    } else if (name.startsWith("check")) {
                        // Non-generic methods (named methods that which already tell us the permission type)
                        var callers = callerInfoByMethod.computeIfAbsent(name, ignored -> new ArrayList<>());
                        callers.add(
                            new CallerInfo(
                                moduleName,
                                Path.of(sourcePath, source).toString(),
                                line,
                                className,
                                methodName,
                                callerIsPublic,
                                null,
                                null
                            )
                        );
                    }
                }
            }
        }

        private String getPackageName(String className) {
            return ClassDesc.ofInternalName(className).packageName();
        }

        @Override
        public void visitParameter(String name, int access) {
            if (name != null) super.visitParameter(name, access);
        }

        @Override
        public void visitLineNumber(int line, Label start) {
            super.visitLineNumber(line, start);
            this.line = line;
        }

        @Override
        public void visitEnd() {
            super.visitEnd();
            if (callsTarget) {
                // System.out.println(traceMethodVisitor.p.getText());
            }
        }
    }
}
