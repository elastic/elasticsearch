/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tree;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * Checks the thread-safety rules documented on {@link Node}. A plan, and the {@link EsField}s it refers to, are shared by every
 * thread that holds it, for example the drivers for all shards on a data node. A lazily computed field that is published unsafely
 * can be seen half-built by another thread; on weakly ordered hardware like aarch64 that is enough to break a query (see #164188).
 * These races almost never reproduce in a test, so this checks the code's shape instead. It reads the bytecode with ASM
 * rather than using reflection, which would need access to private members.
 */
public class PlanFieldSafePublicationTests extends ESTestCase {

    private static final String ESQL_PACKAGE = "org.elasticsearch.xpack.esql.";

    /**
     * Every instance field of a plan class, or of a class nested in one, is {@code final} or {@code volatile}. A plain field that
     * is assigned lazily can be seen by another thread before the object it refers to is complete.
     */
    public void testFieldsAreFinalOrVolatile() throws IOException {
        Set<String> violations = new TreeSet<>();
        for (ClassNode c : planClasses().values()) {
            for (FieldNode f : c.fields) {
                if (isLazy(f) && (f.access & (Opcodes.ACC_VOLATILE | Opcodes.ACC_SYNTHETIC)) == 0) {
                    violations.add(c.name.replace('/', '.') + "#" + f.name);
                }
            }
        }
        assertThat(
            "plan nodes are shared between threads: make these fields final, or volatile if they are computed lazily (see Node)",
            violations,
            empty()
        );
    }

    /**
     * A lazily computed field is assigned once, when its value is complete. Once a method has assigned such a field, the only
     * read of it allowed in that method is the one it returns. Anything else means the value is still being built or changed
     * after other threads can see it, like filling in an array after assigning it, or storing a partial result in the field.
     */
    public void testLazyFieldsArePublishedOnce() throws IOException {
        Map<String, ClassNode> planClasses = planClasses();
        Set<String> violations = new TreeSet<>();
        for (ClassNode c : planClasses.values()) {
            for (MethodNode method : c.methods) {
                if (method.name.equals("<init>") || method.name.equals("<clinit>")) {
                    continue;
                }
                Set<String> assigned = new HashSet<>();
                for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null; insn = insn.getNext()) {
                    if (insn instanceof FieldInsnNode access && isLazyField(access, planClasses)) {
                        String field = access.owner + "#" + access.name;
                        if (access.getOpcode() == Opcodes.PUTFIELD) {
                            assigned.add(field);
                        } else if (access.getOpcode() == Opcodes.GETFIELD && assigned.contains(field) && isReturned(access) == false) {
                            violations.add(c.name.replace('/', '.') + "#" + method.name + " reads " + access.name + " after assigning it");
                        }
                    }
                }
            }
        }
        assertThat(
            "plan nodes are shared between threads: compute lazy values in a local and assign the field once, when complete (see Node)",
            violations,
            empty()
        );
    }

    /**
     * All subclasses of {@link Node} and {@link EsField}, their superclasses in ES|QL, and the inner and anonymous classes
     * declared in them. An instance of an inner or anonymous class, like a memoizing supplier, belongs to the node that
     * created it. Static nested classes don't: they are things like evaluators and scratch buffers, which each driver
     * builds for itself.
     * <p>
     * The result maps each class's internal name, like {@code org/elasticsearch/xpack/esql/core/tree/Node}, to its bytecode.
     */
    private static Map<String, ClassNode> planClasses() throws IOException {
        Set<Class<?>> roots = new LinkedHashSet<>();
        roots.addAll(EsqlNodeSubclassTests.subclassesOf(Node.class));
        roots.addAll(EsqlNodeSubclassTests.subclassesOf(EsField.class));
        Set<Class<?>> result = new LinkedHashSet<>();
        for (Class<?> root : roots) {
            for (Class<?> c = root; c != null && c.getName().startsWith(ESQL_PACKAGE); c = c.getSuperclass()) {
                if (result.add(c) == false) {
                    break;
                }
                for (Class<?> member : c.getNestMembers()) {
                    if (member.getName().startsWith(c.getName() + "$") && Modifier.isStatic(member.getModifiers()) == false) {
                        result.add(member);
                    }
                }
            }
        }
        Map<String, ClassNode> classes = new LinkedHashMap<>();
        for (Class<?> c : result) {
            ClassNode node = readClass(c);
            classes.put(node.name, node);
        }
        return classes;
    }

    /**
     * Can this field be assigned after construction?
     */
    private static boolean isLazy(FieldNode f) {
        return (f.access & (Opcodes.ACC_STATIC | Opcodes.ACC_FINAL)) == 0;
    }

    /**
     * Is {@code access} reading or writing a field of a plan class that can be assigned after construction? The instruction
     * names the class it accesses the field through, which may be a subclass of the one that declares it.
     */
    private static boolean isLazyField(FieldInsnNode access, Map<String, ClassNode> planClasses) {
        for (ClassNode c = planClasses.get(access.owner); c != null; c = planClasses.get(c.superName)) {
            for (FieldNode f : c.fields) {
                if (f.name.equals(access.name)) {
                    return isLazy(f);
                }
            }
        }
        // not a plan class, or declared outside ES|QL
        return false;
    }

    /**
     * Is the value read by {@code access} returned right away? Casting or unboxing it first is fine.
     */
    private static boolean isReturned(FieldInsnNode access) {
        AbstractInsnNode next = nextInstruction(access);
        while (next != null && (next.getOpcode() == Opcodes.CHECKCAST || isUnboxing(next))) {
            next = nextInstruction(next);
        }
        return next != null && next.getOpcode() >= Opcodes.IRETURN && next.getOpcode() <= Opcodes.ARETURN;
    }

    private static AbstractInsnNode nextInstruction(AbstractInsnNode insn) {
        AbstractInsnNode next = insn.getNext();
        while (next != null && next.getOpcode() < 0) {
            // labels, line numbers and frames aren't instructions
            next = next.getNext();
        }
        return next;
    }

    private static boolean isUnboxing(AbstractInsnNode insn) {
        return insn instanceof MethodInsnNode call
            && call.getOpcode() == Opcodes.INVOKEVIRTUAL
            && call.owner.startsWith("java/lang/")
            && call.name.endsWith("Value")
            && call.desc.startsWith("()");
    }

    private static ClassNode readClass(Class<?> c) throws IOException {
        String resource = c.getName().replace('.', '/') + ".class";
        try (InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull("can't read [" + resource + "]", in);
            ClassNode node = new ClassNode();
            new ClassReader(in).accept(node, ClassReader.SKIP_FRAMES);
            return node;
        }
    }
}
