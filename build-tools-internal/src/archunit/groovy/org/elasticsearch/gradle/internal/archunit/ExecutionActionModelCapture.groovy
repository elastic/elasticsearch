/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.invocation.Gradle
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type
import org.objectweb.asm.tree.AbstractInsnNode
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.FieldInsnNode
import org.objectweb.asm.tree.InvokeDynamicInsnNode
import org.objectweb.asm.tree.LineNumberNode
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.tree.MethodNode
import org.objectweb.asm.tree.VarInsnNode

/**
 * Checks only direct lambda/constructor arguments immediately supplied to Task.doFirst/doLast/onlyIf.
 * The API owner and action descriptor distinguish execution actions from configuration Actions.
 * Capturing Project/Gradle in these objects is itself unsupported, even without a subsequent call.
 * This is not a general dataflow analysis: actions stored in locals, model aliases typed as Object,
 * enclosing-instance captures and helpers are deliberately outside this high-confidence ratchet.
 */
@CompileStatic
final class ExecutionActionModelCapture {
    static final class Finding {
        final String owner
        final String method
        final int line
        final String action
        final String capture

        Finding(String owner, String method, int line, String action, String capture) {
            this.owner = owner
            this.method = method
            this.line = line
            this.action = action
            this.capture = capture
        }

        String owner() { owner }
        String method() { method }
        int line() { line }
        String action() { action }
        String capture() { capture }
    }

    static List<Finding> scan(Class<?> type) throws IOException {
        return scan(type.name, type.classLoader)
    }

    static List<Finding> scan(String name, ClassLoader loader) throws IOException {
        ClassNode node = read(name, loader)
        List<Finding> findings = []
        for (MethodNode method : node.methods) {
            int line = 0
            for (AbstractInsnNode instruction : method.instructions) {
                if (instruction instanceof LineNumberNode) {
                    line = ((LineNumberNode) instruction).line
                }
                if (instruction instanceof MethodInsnNode && isExecutionCall((MethodInsnNode) instruction, loader)) {
                    MethodInsnNode call = (MethodInsnNode) instruction
                    AbstractInsnNode argument = previousCode(call)
                    List<Type> captures = []
                    if (argument instanceof InvokeDynamicInsnNode
                        && ((InvokeDynamicInsnNode) argument).bsm.owner == 'java/lang/invoke/LambdaMetafactory') {
                        captures = Arrays.asList(Type.getArgumentTypes(((InvokeDynamicInsnNode) argument).desc))
                    } else if (argument instanceof MethodInsnNode && ((MethodInsnNode) argument).name == '<init>') {
                        captures = constructorCaptures((MethodInsnNode) argument, loader)
                    }
                    for (Type capture : captures) {
                        if (isModel(capture, loader)) {
                            findings.add(new Finding(name, method.name, line, call.name, capture.className))
                        }
                    }
                }
            }
        }
        return findings
    }

    private static ClassNode read(String name, ClassLoader loader) throws IOException {
        ClassNode node = new ClassNode()
        InputStream stream = loader.getResourceAsStream(name.replace('.', '/') + '.class')
        if (stream == null) {
            throw new IOException("Cannot inspect compiled execution actions in ${name}")
        }
        try {
            new ClassReader(stream).accept(node, ClassReader.SKIP_FRAMES)
        } finally {
            stream.close()
        }
        return node
    }

    private static AbstractInsnNode previousCode(AbstractInsnNode instruction) {
        AbstractInsnNode previous = instruction.previous
        while (previous != null && previous.opcode < 0) {
            previous = previous.previous
        }
        return previous
    }

    private static boolean isModel(Type type, ClassLoader loader) {
        return assignable(type, Project, loader) || assignable(type, Gradle, loader)
    }

    private static List<Type> constructorCaptures(MethodInsnNode call, ClassLoader loader) throws IOException {
        // A Project constructor argument alone is not a capture: a valid action can transform it
        // into a String at configuration time. Require a direct this.field = modelParameter store.
        List<Type> captures = []
        for (MethodNode constructor : read(call.owner.replace('/', '.'), loader).methods) {
            if (constructor.name != '<init>' || constructor.desc != call.desc) {
                continue
            }
            int slot = 1
            for (Type parameter : Type.getArgumentTypes(call.desc)) {
                if (isModel(parameter, loader)) {
                    for (AbstractInsnNode instruction : constructor.instructions) {
                        if (instruction instanceof FieldInsnNode) {
                            FieldInsnNode field = (FieldInsnNode) instruction
                            if (field.opcode == Opcodes.PUTFIELD && field.owner == call.owner
                                && isModel(Type.getType(field.desc), loader)) {
                                AbstractInsnNode value = previousCode(field)
                                AbstractInsnNode receiver = previousCode(value)
                                if (value instanceof VarInsnNode && value.opcode == Opcodes.ALOAD
                                    && ((VarInsnNode) value).var == slot && receiver instanceof VarInsnNode
                                    && receiver.opcode == Opcodes.ALOAD && ((VarInsnNode) receiver).var == 0) {
                                    captures.add(parameter)
                                    break
                                }
                            }
                        }
                    }
                }
                slot += parameter.size
            }
        }
        return captures
    }

    private static boolean isExecutionCall(MethodInsnNode call, ClassLoader loader) {
        if (['doFirst', 'doLast', 'onlyIf'].contains(call.name) == false
            || assignable(Type.getObjectType(call.owner), Task, loader) == false) {
            return false
        }
        Type[] arguments = Type.getArgumentTypes(call.desc)
        if (arguments.length == 0) {
            return false
        }
        String action = arguments[arguments.length - 1].className
        return action == 'org.gradle.api.Action' || action == 'org.gradle.api.specs.Spec'
    }

    private static boolean assignable(Type type, Class<?> target, ClassLoader loader) {
        if (type.sort != Type.OBJECT) {
            return false
        }
        try {
            return target.isAssignableFrom(Class.forName(type.className, false, loader))
        } catch (ClassNotFoundException e) {
            return false
        }
    }
}
