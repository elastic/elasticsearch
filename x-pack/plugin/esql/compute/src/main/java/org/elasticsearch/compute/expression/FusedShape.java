/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * The structure of a fused expression tree, independent of the concrete values it is built with: which
 * {@link EvaluatorKernel kernels} are called, how their results feed each other, and which primitive type
 * each leaf input delivers. Two expression trees with the same shape, such as {@code (a + b) * c} and
 * {@code (x + y) * 2}, share one spun evaluator class; only their leaf evaluators, fixed values and
 * warning locations differ.
 *
 * @param root the kernel that produces the fused result
 * @param leafCount the number of leaf inputs, each read from a block for every page
 */
record FusedShape(KernelNode root, int leafCount) {
    /**
     * The largest tree that is fused. Deeper trees are evaluated unfused; this bounds the size of the
     * spun method and the number of distinct shapes a workload can produce.
     */
    static final int MAX_NODES = 64;

    /**
     * The primitive types a kernel can consume or produce, with the block, vector and builder classes that
     * carry them.
     */
    enum Kind {
        INT(
            int.class,
            Integer.class,
            IntBlock.class,
            IntVector.class,
            IntBlock.Builder.class,
            "Int",
            ElementType.INT,
            Type.INT_TYPE,
            Opcodes.ICONST_0
        ),
        LONG(
            long.class,
            Long.class,
            LongBlock.class,
            LongVector.class,
            LongBlock.Builder.class,
            "Long",
            ElementType.LONG,
            Type.LONG_TYPE,
            Opcodes.LCONST_0
        ),
        DOUBLE(
            double.class,
            Double.class,
            DoubleBlock.class,
            DoubleVector.class,
            DoubleBlock.Builder.class,
            "Double",
            ElementType.DOUBLE,
            Type.DOUBLE_TYPE,
            Opcodes.DCONST_0
        ),
        FLOAT(
            float.class,
            Float.class,
            FloatBlock.class,
            FloatVector.class,
            FloatBlock.Builder.class,
            "Float",
            ElementType.FLOAT,
            Type.FLOAT_TYPE,
            Opcodes.FCONST_0
        ),
        BOOLEAN(
            boolean.class,
            Boolean.class,
            BooleanBlock.class,
            BooleanVector.class,
            BooleanBlock.Builder.class,
            "Boolean",
            ElementType.BOOLEAN,
            Type.BOOLEAN_TYPE,
            Opcodes.ICONST_0
        );

        final Class<?> primitive;
        final Class<?> box;
        final Class<?> blockClass;
        final Class<?> vectorClass;
        final Class<?> builderClass;
        /**
         * The block and vector getter, {@code getInt} and so on.
         */
        final String getter;
        /**
         * The builder method, {@code appendInt} and so on.
         */
        final String appender;
        final ElementType elementType;
        /**
         * How many local variable slots a value takes.
         */
        final int slots;
        final int loadOpcode;
        final int storeOpcode;
        /**
         * The instruction pushing the type's zero.
         */
        final int zeroOpcode;

        Kind(
            Class<?> primitive,
            Class<?> box,
            Class<?> blockClass,
            Class<?> vectorClass,
            Class<?> builderClass,
            String name,
            ElementType elementType,
            Type asmType,
            int zeroOpcode
        ) {
            this.primitive = primitive;
            this.box = box;
            this.blockClass = blockClass;
            this.vectorClass = vectorClass;
            this.builderClass = builderClass;
            this.getter = "get" + name;
            this.appender = "append" + name;
            this.elementType = elementType;
            this.slots = asmType.getSize();
            this.loadOpcode = asmType.getOpcode(Opcodes.ILOAD);
            this.storeOpcode = asmType.getOpcode(Opcodes.ISTORE);
            this.zeroOpcode = zeroOpcode;
        }

        /**
         * The kind carrying {@code type}, or {@code null} if no block type carries it.
         */
        static Kind of(Class<?> type) {
            for (Kind kind : values()) {
                if (kind.primitive == type) {
                    return kind;
                }
            }
            return null;
        }
    }

    sealed interface Node permits KernelNode, LeafNode {}

    /**
     * A kernel call. Children are listed in the order of the kernel's value parameters.
     *
     * @param index the node's position in post-order, which is also its evaluation order and the index of its
     *              warning location
     */
    record KernelNode(int index, EvaluatorKernel kernel, List<Node> inputs) implements Node {
        Kind resultKind() {
            return Kind.of(kernel.resultType());
        }

        int fixedCount() {
            return kernel.parameters().size() - kernel.valueParameterCount();
        }
    }

    /**
     * A value read from a leaf evaluator's block.
     *
     * @param index the leaf's position among all leaves, left to right
     * @param kind the type the consuming kernel expects
     */
    record LeafNode(int index, Kind kind) implements Node {}

    /**
     * All kernel nodes in post-order, so every node comes after its inputs.
     */
    List<KernelNode> nodes() {
        List<KernelNode> nodes = new ArrayList<>();
        collect(root, nodes);
        return nodes;
    }

    private static void collect(Node node, List<KernelNode> into) {
        if (node instanceof KernelNode kernelNode) {
            for (Node input : kernelNode.inputs()) {
                collect(input, into);
            }
            into.add(kernelNode);
        }
    }

    /**
     * All leaves, ordered by {@link LeafNode#index()}.
     */
    List<LeafNode> leaves() {
        List<LeafNode> leaves = new ArrayList<>();
        collectLeaves(root, leaves);
        return leaves;
    }

    private static void collectLeaves(Node node, List<LeafNode> into) {
        if (node instanceof KernelNode kernelNode) {
            for (Node input : kernelNode.inputs()) {
                collectLeaves(input, into);
            }
        } else if (node instanceof LeafNode leaf) {
            into.add(leaf);
        }
    }

    /**
     * Whether a kernel's signature can be fused: every value is carried by a {@link Kind}, and every class the
     * spun code has to name is public.
     */
    static boolean supports(EvaluatorKernel kernel) {
        if (Kind.of(kernel.resultType()) == null || isAccessible(kernel.evaluatorClass()) == false) {
            return false;
        }
        for (EvaluatorKernel.Parameter parameter : kernel.parameters()) {
            Class<?> type = parameter.type();
            if (parameter.fixed()) {
                if (type.isPrimitive() ? Kind.of(type) == null : isAccessible(type) == false) {
                    return false;
                }
            } else if (Kind.of(type) == null) {
                return false;
            }
        }
        for (Class<? extends Exception> exception : kernel.warnExceptions()) {
            if (isAccessible(exception) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether code in another package can name {@code type}: it and all its enclosing classes are public.
     */
    private static boolean isAccessible(Class<?> type) {
        while (type.isArray()) {
            type = type.getComponentType();
        }
        if (type.isPrimitive()) {
            return true;
        }
        for (Class<?> c = type; c != null; c = c.getEnclosingClass()) {
            if (Modifier.isPublic(c.getModifiers()) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks a factory tree, turning every {@link FusableEvaluatorFactory} whose kernel fits into a
     * {@link KernelNode} and everything else into a {@link LeafNode}. Collects, in the same order the spun
     * class expects them, the leaf factories, the fixed values and the warning locations.
     */
    static final class Builder {
        private final List<ExpressionEvaluator.Factory> leaves = new ArrayList<>();
        private final List<Object> fixedValues = new ArrayList<>();
        private final List<WarningSourceLocation> sources = new ArrayList<>();
        private int nodeCount;

        /**
         * The shape of {@code root}, or {@code null} if it can't be fused at all.
         */
        FusedShape build(ExpressionEvaluator.Factory root) {
            if (fusable(root) == null) {
                return null;
            }
            Node node = node(root, null);
            if (node instanceof KernelNode kernelNode && nodeCount <= MAX_NODES) {
                return new FusedShape(kernelNode, leaves.size());
            }
            return null;
        }

        List<ExpressionEvaluator.Factory> leaves() {
            return leaves;
        }

        List<Object> fixedValues() {
            return fixedValues;
        }

        List<WarningSourceLocation> sources() {
            return sources;
        }

        private Node node(ExpressionEvaluator.Factory factory, Kind expected) {
            FusableEvaluatorFactory fusable = fusable(factory);
            if (fusable == null || (expected != null && Kind.of(fusable.kernel().resultType()) != expected)) {
                leaves.add(factory);
                return new LeafNode(leaves.size() - 1, expected);
            }
            EvaluatorKernel kernel = fusable.kernel();
            List<Node> inputs = new ArrayList<>(kernel.valueParameterCount());
            int valueIndex = 0;
            for (EvaluatorKernel.Parameter parameter : kernel.parameters()) {
                if (parameter.fixed() == false) {
                    inputs.add(node(fusable.inputs().get(valueIndex++), Kind.of(parameter.type())));
                }
            }
            fixedValues.addAll(fusable.fixedValues());
            sources.add(fusable.source());
            return new KernelNode(nodeCount++, kernel, List.copyOf(inputs));
        }

        /**
         * {@code factory} as a fusable factory whose kernel and structure the spun code can use, else {@code null}.
         */
        private static FusableEvaluatorFactory fusable(ExpressionEvaluator.Factory factory) {
            if (factory instanceof FusableEvaluatorFactory fusable) {
                EvaluatorKernel kernel = fusable.kernel();
                if (supports(kernel)
                    && fusable.inputs().size() == kernel.valueParameterCount()
                    && fusable.fixedValues().size() == kernel.parameters().size() - kernel.valueParameterCount()) {
                    return fusable;
                }
            }
            return null;
        }
    }
}
