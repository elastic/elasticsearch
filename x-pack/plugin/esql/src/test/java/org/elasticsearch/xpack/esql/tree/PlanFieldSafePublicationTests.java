/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tree;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.HashMap;
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
     * Methods that {@link #testLazyFieldsArePublishedOnce} lets through for now, each with the PR that fixes it. The test fails
     * once an entry no longer breaks the rules, so remove it when that PR is merged.
     */
    private static final Map<String, String> KNOWN_VIOLATIONS = Map.of(
        "org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo#dataType",
        "https://github.com/elastic/elasticsearch/pull/166331"
    );

    /**
     * Classes, other than enums, records and {@link Node}s, whose instances are safe to publish without {@code volatile}:
     * everything they hold is reachable through {@code final} fields, so a thread that sees a reference to one also sees
     * its contents. {@link #testSafeTypesOnlyHaveFinalFields} checks the ES|QL ones.
     */
    private static final Set<Class<?>> SAFE_CLASSES = Set.of(
        String.class,
        Boolean.class,
        Byte.class,
        Character.class,
        Short.class,
        Integer.class,
        Long.class,
        Float.class,
        Double.class,
        AttributeSet.class,
        Expression.TypeResolution.class
    );

    /**
     * Every instance field of a plan class, or of a class nested in one, is {@code final}, {@code volatile}, or holds a type
     * that is safe to publish without {@code volatile} (see {@link #isSafeType}). A plain field that holds anything else, like
     * an {@code ArrayList}, can be seen by another thread before the object it refers to is complete.
     */
    public void testLazyFieldsAreVolatileOrSafe() throws IOException {
        Set<String> violations = new TreeSet<>();
        for (ClassNode c : planClasses().values()) {
            for (FieldNode f : c.fields) {
                if (isLazy(f) && isVolatile(f) == false && isSafeType(Type.getType(f.desc)) == false) {
                    violations.add(c.name.replace('/', '.') + "#" + f.name + " (" + Type.getType(f.desc).getClassName() + ")");
                }
            }
        }
        assertThat(
            "plan nodes are shared between threads: make these fields final, or volatile if they are computed lazily, unless "
                + "their type is immutable (see Node)",
            violations,
            empty()
        );
    }

    /**
     * A lazily computed field is assigned once, when its value is complete. Once a method has assigned such a field, the only
     * read of it allowed in that method is the one it returns. Anything else means the value is still being built or changed
     * after other threads can see it, like filling in an array after assigning it, or storing a partial result in the field.
     * <p>
     * A field that isn't {@code volatile} is also read at most once per method. Two plain reads of the same field can be
     * reordered, so the second one can return {@code null} even when the first one didn't.
     */
    public void testLazyFieldsArePublishedOnce() throws IOException {
        Map<String, ClassNode> planClasses = planClasses();
        Set<String> violations = new TreeSet<>();
        Set<String> violatingMethods = new HashSet<>();
        for (ClassNode c : planClasses.values()) {
            for (MethodNode method : c.methods) {
                if (method.name.equals("<init>") || method.name.equals("<clinit>")) {
                    continue;
                }
                String where = c.name.replace('/', '.') + "#" + method.name;
                Set<FieldNode> assigned = new HashSet<>();
                Map<FieldNode, Integer> reads = new HashMap<>();
                for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null; insn = insn.getNext()) {
                    if (insn instanceof FieldInsnNode access) {
                        FieldNode field = lazyField(access, planClasses);
                        if (field == null) {
                            continue;
                        }
                        if (access.getOpcode() == Opcodes.PUTFIELD) {
                            assigned.add(field);
                        } else if (access.getOpcode() == Opcodes.GETFIELD) {
                            if (assigned.contains(field) && isReturned(access) == false) {
                                violatingMethods.add(where);
                                violations.add(where + " reads " + field.name + " after assigning it");
                            }
                            if (isVolatile(field) == false && reads.merge(field, 1, Integer::sum) > 1) {
                                violatingMethods.add(where);
                                violations.add(where + " reads " + field.name + " more than once");
                            }
                        }
                    }
                }
            }
        }
        violations.removeIf(v -> KNOWN_VIOLATIONS.containsKey(v.substring(0, v.indexOf(' '))));
        assertThat(
            "plan nodes are shared between threads: read lazy fields into a local once, compute the value in a local and "
                + "assign the field once, when complete (see Node)",
            violations,
            empty()
        );
        Set<String> fixed = new TreeSet<>(KNOWN_VIOLATIONS.keySet());
        fixed.removeAll(violatingMethods);
        assertThat("these methods follow the rules now: remove them from KNOWN_VIOLATIONS", fixed, empty());
    }

    /**
     * The ES|QL classes and enums that plain lazy fields rely on to be safe without {@code volatile} only have {@code final}
     * instance fields, so the guarantee for {@code final} fields covers everything they hold.
     */
    public void testSafeTypesOnlyHaveFinalFields() throws IOException {
        Set<Class<?>> types = new LinkedHashSet<>(SAFE_CLASSES);
        for (ClassNode c : planClasses().values()) {
            for (FieldNode f : c.fields) {
                Type type = Type.getType(f.desc);
                if (isLazy(f) && isVolatile(f) == false && type.getSort() == Type.OBJECT) {
                    Class<?> fieldClass = loadClass(type.getClassName());
                    if (fieldClass.isEnum()) {
                        types.add(fieldClass);
                    }
                }
            }
        }
        Set<String> violations = new TreeSet<>();
        for (Class<?> type : types) {
            for (Class<?> c = type; c != null && c.getName().startsWith(ESQL_PACKAGE); c = c.getSuperclass()) {
                for (FieldNode f : readClass(c).fields) {
                    if (isLazy(f) && (f.access & Opcodes.ACC_SYNTHETIC) == 0) {
                        violations.add(c.getName() + "#" + f.name);
                    }
                }
            }
        }
        assertThat(
            "plain lazy fields rely on these types being immutable: make these fields final, or remove the type from SAFE_CLASSES",
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

    private static boolean isVolatile(FieldNode f) {
        return (f.access & Opcodes.ACC_VOLATILE) != 0;
    }

    /**
     * Can a value of this type be published through a plain field? Only if a thread that sees the reference also sees the
     * object's contents, which holds when everything it holds is reachable through {@code final} fields: enums, records,
     * {@link Node}s (whose own lazy fields follow these same rules) and {@link #SAFE_CLASSES}. Writes to {@code long}
     * and {@code double} fields aren't atomic, so those need {@code volatile} too.
     */
    private static boolean isSafeType(Type type) {
        return switch (type.getSort()) {
            case Type.BOOLEAN, Type.CHAR, Type.BYTE, Type.SHORT, Type.INT, Type.FLOAT -> true;
            case Type.LONG, Type.DOUBLE, Type.ARRAY -> false;
            case Type.OBJECT -> {
                Class<?> c = loadClass(type.getClassName());
                yield c.isEnum() || c.isRecord() || Node.class.isAssignableFrom(c) || SAFE_CLASSES.contains(c);
            }
            default -> throw new AssertionError("not a field type: " + type);
        };
    }

    /**
     * The field that {@code access} reads or writes, if it's a field of a plan class that can be assigned after construction,
     * or {@code null} otherwise. The instruction names the class it accesses the field through, which may be a subclass of
     * the one that declares it.
     */
    private static FieldNode lazyField(FieldInsnNode access, Map<String, ClassNode> planClasses) {
        for (ClassNode c = planClasses.get(access.owner); c != null; c = planClasses.get(c.superName)) {
            for (FieldNode f : c.fields) {
                if (f.name.equals(access.name)) {
                    return isLazy(f) ? f : null;
                }
            }
        }
        // not a plan class, or declared outside ES|QL
        return null;
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

    private static Class<?> loadClass(String name) {
        try {
            return Class.forName(name, false, PlanFieldSafePublicationTests.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new AssertionError("can't load [" + name + "]", e);
        }
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
