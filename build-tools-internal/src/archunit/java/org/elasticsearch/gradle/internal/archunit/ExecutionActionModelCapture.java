/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.archunit;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.invocation.Gradle;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Checks only direct lambda/constructor arguments immediately supplied to Task.doFirst/doLast/onlyIf.
 * The API owner and action descriptor distinguish execution actions from configuration Actions.
 * Capturing Project/Gradle in these objects is itself unsupported, even without a subsequent call.
 * This is not a general dataflow analysis: actions stored in locals, model aliases typed as Object,
 * enclosing-instance captures and helpers are deliberately outside this high-confidence ratchet.
 */
final class ExecutionActionModelCapture {
    record Finding(String owner, String method, int line, String action, String capture) {}

    static List<Finding> scan(Class<?> type) throws IOException {
        return scan(type.getName(), type.getClassLoader());
    }

    static List<Finding> scan(String name, ClassLoader loader) throws IOException {
        ClassNode node = read(name, loader);
        List<Finding> findings = new ArrayList<>();
        for (MethodNode method : node.methods) {
            int line = 0;
            for (AbstractInsnNode instruction : method.instructions) {
                if (instruction instanceof LineNumberNode number) {
                    line = number.line;
                }
                if (instruction instanceof MethodInsnNode call && isExecutionCall(call, loader)) {
                    AbstractInsnNode argument = previousCode(call);
                    List<Type> captures = List.of();
                    if (argument instanceof InvokeDynamicInsnNode lambda
                        && lambda.bsm.getOwner().equals("java/lang/invoke/LambdaMetafactory")) {
                        captures = List.of(Type.getArgumentTypes(lambda.desc));
                    } else if (argument instanceof MethodInsnNode constructor && constructor.name.equals("<init>")) {
                        captures = constructorCaptures(constructor, loader);
                    }
                    for (Type capture : captures) {
                        if (isModel(capture, loader)) {
                            findings.add(new Finding(name, method.name, line, call.name, capture.getClassName()));
                        }
                    }
                }
            }
        }
        return findings;
    }

    private static ClassNode read(String name, ClassLoader loader) throws IOException {
        ClassNode node = new ClassNode();
        try (InputStream stream = loader.getResourceAsStream(name.replace('.', '/') + ".class")) {
            if (stream == null) {
                throw new IOException("Cannot inspect compiled execution actions in " + name);
            }
            new ClassReader(stream).accept(node, ClassReader.SKIP_FRAMES);
        }
        return node;
    }

    private static AbstractInsnNode previousCode(AbstractInsnNode instruction) {
        AbstractInsnNode previous = instruction.getPrevious();
        while (previous != null && previous.getOpcode() < 0) {
            previous = previous.getPrevious();
        }
        return previous;
    }

    private static boolean isModel(Type type, ClassLoader loader) {
        return assignable(type, Project.class, loader) || assignable(type, Gradle.class, loader);
    }

    private static List<Type> constructorCaptures(MethodInsnNode call, ClassLoader loader) throws IOException {
        // A Project constructor argument alone is not a capture: a valid action can transform it
        // into a String at configuration time. Require a direct this.field = modelParameter store.
        List<Type> captures = new ArrayList<>();
        for (MethodNode constructor : read(call.owner.replace('/', '.'), loader).methods) {
            if (constructor.name.equals("<init>") == false || constructor.desc.equals(call.desc) == false) {
                continue;
            }
            int slot = 1;
            for (Type parameter : Type.getArgumentTypes(call.desc)) {
                if (isModel(parameter, loader)) {
                    for (AbstractInsnNode instruction : constructor.instructions) {
                        if (instruction instanceof FieldInsnNode field
                            && field.getOpcode() == Opcodes.PUTFIELD
                            && field.owner.equals(call.owner)
                            && isModel(Type.getType(field.desc), loader)
                            && previousCode(field) instanceof VarInsnNode value
                            && value.getOpcode() == Opcodes.ALOAD
                            && value.var == slot
                            && previousCode(value) instanceof VarInsnNode receiver
                            && receiver.getOpcode() == Opcodes.ALOAD
                            && receiver.var == 0) {
                            captures.add(parameter);
                            break;
                        }
                    }
                }
                slot += parameter.getSize();
            }
        }
        return captures;
    }

    private static boolean isExecutionCall(MethodInsnNode call, ClassLoader loader) {
        if (Set.of("doFirst", "doLast", "onlyIf").contains(call.name) == false
            || assignable(Type.getObjectType(call.owner), Task.class, loader) == false) {
            return false;
        }
        Type[] arguments = Type.getArgumentTypes(call.desc);
        if (arguments.length == 0) {
            return false;
        }
        String action = arguments[arguments.length - 1].getClassName();
        return action.equals("org.gradle.api.Action") || action.equals("org.gradle.api.specs.Spec");
    }

    private static boolean assignable(Type type, Class<?> target, ClassLoader loader) {
        if (type.getSort() != Type.OBJECT) {
            return false;
        }
        try {
            return target.isAssignableFrom(Class.forName(type.getClassName(), false, loader));
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
