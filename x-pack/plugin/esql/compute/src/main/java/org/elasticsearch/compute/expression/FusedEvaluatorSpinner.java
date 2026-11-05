/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.expression.FusedShape.KernelNode;
import org.elasticsearch.compute.expression.FusedShape.Kind;
import org.elasticsearch.compute.expression.FusedShape.LeafNode;
import org.elasticsearch.compute.expression.FusedShape.Node;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Spins, at runtime, one hidden subclass of {@link FusedEvaluator} per {@link FusedShape}. The subclass
 * implements the per-position loop for that shape by calling the kernels' {@code process} forwarders
 * directly, so the JIT compiles the whole expression tree as one method.
 * <p>
 * For the shape of {@code (a + b) * c} over {@code int}s the emitted class is equivalent to:
 * <pre>{@code
 * public final class MulIntsEvaluator$Fused extends FusedEvaluator {
 *     public MulIntsEvaluator$Fused(DriverContext ctx, ExpressionEvaluator[] leaves, WarningSourceLocation[] sources,
 *             ElementType resultType, String description, Object[] fixed) {
 *         super(ctx, leaves, sources, resultType, description);
 *     }
 *
 *     protected final void eval(int positionCount, Block[] blocks, Block.Builder result) {
 *         IntBlock b0 = (IntBlock) blocks[0];
 *         IntBlock b1 = (IntBlock) blocks[1];
 *         IntBlock b2 = (IntBlock) blocks[2];
 *         IntVector v0 = b0.asVector();
 *         if (v0 == null) { evalBlocks(positionCount, (IntBlock.Builder) result, b0, b1, b2); return; }
 *         ... same for v1 and v2 ...
 *         evalVectors(positionCount, (IntBlock.Builder) result, v0, v1, v2);
 *     }
 *
 *     private void evalBlocks(int positionCount, IntBlock.Builder result, IntBlock b0, IntBlock b1, IntBlock b2) {
 *         for (int p = 0; p < positionCount; p++) {
 *             // node 0: a + b
 *             boolean null0 = false; int value0 = 0;
 *             if (b0.isNull(p)) { null0 = true; } else if (b0.getValueCount(p) != 1) { warnMultiValue(0); null0 = true; }
 *             else if (b1.isNull(p)) { null0 = true; } else if (b1.getValueCount(p) != 1) { warnMultiValue(0); null0 = true; }
 *             else {
 *                 try { value0 = AddIntsEvaluator.process(b0.getInt(b0.getFirstValueIndex(p)), b1.getInt(b1.getFirstValueIndex(p))); }
 *                 catch (ArithmeticException e) { warnException(0, e); null0 = true; }
 *             }
 *             // node 1: node0 * c
 *             boolean null1 = false; int value1 = 0;
 *             if (null0) { null1 = true; }
 *             else if (b2.isNull(p)) { null1 = true; } else if (b2.getValueCount(p) != 1) { warnMultiValue(1); null1 = true; }
 *             else {
 *                 try { value1 = MulIntsEvaluator.process(value0, b2.getInt(b2.getFirstValueIndex(p))); }
 *                 catch (ArithmeticException e) { warnException(1, e); null1 = true; }
 *             }
 *             if (null1) { result.appendNull(); } else { result.appendInt(value1); }
 *         }
 *     }
 *
 *     private void evalVectors(int positionCount, IntBlock.Builder result, IntVector v0, IntVector v1, IntVector v2) {
 *         // the same without the null and multi-value checks, reading vN.getInt(p)
 *     }
 * }
 * }</pre>
 * Fixed kernel parameters become {@code private final} fields, assigned from the {@code Object[]} the
 * constructor receives, and are loaded at their position in the kernel's parameter list.
 * <p>
 * The hidden class is defined in the package and class loader of the root kernel's evaluator class, which
 * is where the generated evaluators live, so it can name every kernel class and link against this module's
 * exported {@link FusedEvaluator}. Spun classes are cached per shape and never unloaded; the cache is
 * bounded by {@link #MAX_CACHED_SHAPES}, beyond which trees are evaluated unfused.
 */
final class FusedEvaluatorSpinner {
    private static final Logger logger = LogManager.getLogger(FusedEvaluatorSpinner.class);

    /**
     * The number of distinct shapes this JVM will spin a class for. Each class costs a few KB of metaspace
     * for its lifetime; shapes are structural, so one class serves every query with the same tree of kernels.
     */
    static final int MAX_CACHED_SHAPES = 1024;

    static final FusedEvaluatorSpinner SHARED = new FusedEvaluatorSpinner();

    /**
     * The type the constructor handle in the cache has: it builds a {@link FusedEvaluator} from the arguments
     * of {@link FusedEvaluator}'s constructor followed by the flattened fixed values.
     */
    static final MethodType CONSTRUCTOR_TYPE = MethodType.methodType(
        FusedEvaluator.class,
        DriverContext.class,
        ExpressionEvaluator[].class,
        WarningSourceLocation[].class,
        ElementType.class,
        String.class,
        Object[].class
    );

    private static final String SUPER = Type.getInternalName(FusedEvaluator.class);
    private static final String SUPER_CONSTRUCTOR_DESCRIPTOR = Type.getMethodDescriptor(
        Type.VOID_TYPE,
        Type.getType(DriverContext.class),
        Type.getType(ExpressionEvaluator[].class),
        Type.getType(WarningSourceLocation[].class),
        Type.getType(ElementType.class),
        Type.getType(String.class)
    );
    private static final String CONSTRUCTOR_DESCRIPTOR = CONSTRUCTOR_TYPE.changeReturnType(void.class).toMethodDescriptorString();
    private static final String EVAL_DESCRIPTOR = Type.getMethodDescriptor(
        Type.VOID_TYPE,
        Type.INT_TYPE,
        Type.getType(Block[].class),
        Type.getType(Block.Builder.class)
    );
    private static final String WARN_MULTI_VALUE_DESCRIPTOR = Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE);
    private static final String WARN_EXCEPTION_DESCRIPTOR = Type.getMethodDescriptor(
        Type.VOID_TYPE,
        Type.INT_TYPE,
        Type.getType(Exception.class)
    );
    private static final String INT_TO_INT = Type.getMethodDescriptor(Type.INT_TYPE, Type.INT_TYPE);
    private static final String INT_TO_BOOLEAN = Type.getMethodDescriptor(Type.BOOLEAN_TYPE, Type.INT_TYPE);

    private final ConcurrentHashMap<FusedShape, MethodHandle> constructors = new ConcurrentHashMap<>();
    private final LongAdder spins = new LongAdder();

    FusedEvaluatorSpinner() {}

    /**
     * The constructor of the spun class for {@code shape}, typed as {@link #CONSTRUCTOR_TYPE}, or empty if the
     * cache is full or the class couldn't be spun. Concurrent requests for a new shape spin it once.
     */
    Optional<MethodHandle> constructor(FusedShape shape) {
        MethodHandle cached = constructors.get(shape);
        if (cached != null) {
            return Optional.of(cached);
        }
        if (constructors.size() >= MAX_CACHED_SHAPES) {
            return Optional.empty();
        }
        try {
            return Optional.of(constructors.computeIfAbsent(shape, this::spin));
        } catch (RuntimeException | LinkageError e) {
            logger.debug(() -> "failed to spin fused evaluator for [" + shape + "]", e);
            return Optional.empty();
        }
    }

    long spinCount() {
        return spins.sum();
    }

    int cacheSize() {
        return constructors.size();
    }

    /**
     * Spins the class for {@code shape} without consulting the cache, propagating any failure. Package private
     * so tests can see why a shape didn't spin; production goes through {@link #constructor}.
     */
    MethodHandle spin(FusedShape shape) {
        Class<?> host = shape.root().kernel().evaluatorClass();
        ClassLoader loader = host.getClassLoader();
        List<KernelNode> nodes = shape.nodes();
        checkVisible(loader, FusedEvaluator.class);
        for (KernelNode node : nodes) {
            EvaluatorKernel kernel = node.kernel();
            checkVisible(loader, kernel.evaluatorClass());
            for (EvaluatorKernel.Parameter parameter : kernel.parameters()) {
                checkVisible(loader, parameter.type());
            }
            for (Class<? extends Exception> exception : kernel.warnExceptions()) {
                checkVisible(loader, exception);
            }
        }
        String internalName = Type.getInternalName(host) + "$Fused";
        byte[] bytecode = new Emitter(shape, nodes, internalName, loader).emit();
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(host, MethodHandles.lookup());
            // defineHiddenClass is gated by the create_class_loader entitlement, which the ESQL plugin holds.
            MethodHandles.Lookup hidden = lookup.defineHiddenClass(bytecode, true);
            MethodHandle constructor = hidden.findConstructor(hidden.lookupClass(), CONSTRUCTOR_TYPE.changeReturnType(void.class));
            spins.increment();
            return constructor.asType(CONSTRUCTOR_TYPE);
        } catch (IllegalAccessException | NoSuchMethodException e) {
            throw new IllegalStateException("cannot spin fused evaluator in " + host.getName(), e);
        }
    }

    /**
     * The spun class resolves every class it names through {@code loader}; this fails early, with a clear
     * message, if a kernel or a fixed value's type lives somewhere that loader can't see.
     */
    private static void checkVisible(ClassLoader loader, Class<?> type) {
        while (type.isArray()) {
            type = type.getComponentType();
        }
        if (type.isPrimitive()) {
            return;
        }
        try {
            if (Class.forName(type.getName(), false, loader) != type) {
                throw new IllegalArgumentException("[" + type.getName() + "] resolves to a different class in [" + loader + "]");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("[" + type.getName() + "] is not visible from [" + loader + "]", e);
        }
    }

    /**
     * Emits the class for one shape. The structure mirrors the class in the {@link FusedEvaluatorSpinner}
     * documentation: fixed-value fields, a constructor, the {@code eval} dispatcher and one loop method each
     * for the all-vectors and the general case.
     */
    private static final class Emitter {
        private final FusedShape shape;
        private final List<KernelNode> nodes;
        private final List<LeafNode> leaves;
        private final String internalName;
        private final ClassWriter cw;
        /**
         * The index of the first fixed-value field of each node.
         */
        private final int[] firstFixedField;
        private final Class<?>[] fixedTypes;

        Emitter(FusedShape shape, List<KernelNode> nodes, String internalName, ClassLoader loader) {
            this.shape = shape;
            this.nodes = nodes;
            this.leaves = shape.leaves();
            this.internalName = internalName;
            this.cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS) {
                @Override
                protected ClassLoader getClassLoader() {
                    return loader;
                }
            };
            this.firstFixedField = new int[nodes.size()];
            int fixedCount = 0;
            for (KernelNode node : nodes) {
                firstFixedField[node.index()] = fixedCount;
                fixedCount += node.fixedCount();
            }
            this.fixedTypes = new Class<?>[fixedCount];
            int f = 0;
            for (KernelNode node : nodes) {
                for (EvaluatorKernel.Parameter parameter : node.kernel().parameters()) {
                    if (parameter.fixed()) {
                        fixedTypes[f++] = parameter.type();
                    }
                }
            }
        }

        byte[] emit() {
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, internalName, null, SUPER, null);
            for (int f = 0; f < fixedTypes.length; f++) {
                cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, fixedField(f), Type.getDescriptor(fixedTypes[f]), null, null)
                    .visitEnd();
            }
            emitConstructor();
            emitDispatcher();
            emitLoop(true);
            emitLoop(false);
            cw.visitEnd();
            return cw.toByteArray();
        }

        private static String fixedField(int f) {
            return "fixed" + f;
        }

        /**
         * {@code super(ctx, leaves, sources, resultType, description)}, then one field per fixed value, unboxed
         * or cast from the {@code Object[]}.
         */
        private void emitConstructor() {
            MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", CONSTRUCTOR_DESCRIPTOR, null, null);
            mv.visitCode();
            for (int slot = 0; slot <= 5; slot++) {
                mv.visitVarInsn(Opcodes.ALOAD, slot);
            }
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, SUPER, "<init>", SUPER_CONSTRUCTOR_DESCRIPTOR, false);
            for (int f = 0; f < fixedTypes.length; f++) {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitVarInsn(Opcodes.ALOAD, 6);
                pushInt(mv, f);
                mv.visitInsn(Opcodes.AALOAD);
                Class<?> type = fixedTypes[f];
                if (type.isPrimitive()) {
                    Class<?> box = Kind.of(type).box;
                    mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(box));
                    mv.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(box),
                        type.getName() + "Value",
                        Type.getMethodDescriptor(Type.getType(type)),
                        false
                    );
                } else {
                    mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(type));
                }
                mv.visitFieldInsn(Opcodes.PUTFIELD, internalName, fixedField(f), Type.getDescriptor(type));
            }
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        /**
         * Casts the leaf blocks, tries to get a vector from each and calls the matching loop method.
         */
        private void emitDispatcher() {
            MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "eval", EVAL_DESCRIPTOR, null, null);
            mv.visitCode();
            int leafCount = leaves.size();
            int blockSlot = 4;
            int vectorSlot = blockSlot + leafCount;
            for (LeafNode leaf : leaves) {
                mv.visitVarInsn(Opcodes.ALOAD, 2);
                pushInt(mv, leaf.index());
                mv.visitInsn(Opcodes.AALOAD);
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(leaf.kind().blockClass));
                mv.visitVarInsn(Opcodes.ASTORE, blockSlot + leaf.index());
            }
            Label blocksPath = new Label();
            for (LeafNode leaf : leaves) {
                Kind kind = leaf.kind();
                mv.visitVarInsn(Opcodes.ALOAD, blockSlot + leaf.index());
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    Type.getInternalName(kind.blockClass),
                    "asVector",
                    Type.getMethodDescriptor(Type.getType(kind.vectorClass)),
                    true
                );
                mv.visitVarInsn(Opcodes.ASTORE, vectorSlot + leaf.index());
                mv.visitVarInsn(Opcodes.ALOAD, vectorSlot + leaf.index());
                mv.visitJumpInsn(Opcodes.IFNULL, blocksPath);
            }
            emitLoopCall(mv, true, vectorSlot);
            mv.visitLabel(blocksPath);
            emitLoopCall(mv, false, blockSlot);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        private void emitLoopCall(MethodVisitor mv, boolean vectors, int firstLeafSlot) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitVarInsn(Opcodes.ILOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(shape.root().resultKind().builderClass));
            for (int i = 0; i < leaves.size(); i++) {
                mv.visitVarInsn(Opcodes.ALOAD, firstLeafSlot + i);
            }
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, internalName, loopName(vectors), loopDescriptor(vectors), false);
            mv.visitInsn(Opcodes.RETURN);
        }

        private static String loopName(boolean vectors) {
            return vectors ? "evalVectors" : "evalBlocks";
        }

        private String loopDescriptor(boolean vectors) {
            Type[] args = new Type[2 + leaves.size()];
            args[0] = Type.INT_TYPE;
            args[1] = Type.getType(shape.root().resultKind().builderClass);
            for (LeafNode leaf : leaves) {
                args[2 + leaf.index()] = Type.getType(vectors ? leaf.kind().vectorClass : leaf.kind().blockClass);
            }
            return Type.getMethodDescriptor(Type.VOID_TYPE, args);
        }

        /**
         * The per-position loop. In the vectors variant inputs can't be null or multi-valued, so those checks
         * are left out and values are read straight from the vectors.
         */
        private void emitLoop(boolean vectors) {
            MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PRIVATE, loopName(vectors), loopDescriptor(vectors), null, null);
            mv.visitCode();
            // locals: 0 this, 1 positionCount, 2 result, 3.. leaves, then p, then per node a null flag and a value
            int firstLeafSlot = 3;
            int next = firstLeafSlot + leaves.size();
            int positionSlot = next++;
            int[] nullSlot = new int[nodes.size()];
            int[] valueSlot = new int[nodes.size()];
            for (KernelNode node : nodes) {
                nullSlot[node.index()] = next++;
                valueSlot[node.index()] = next;
                next += node.resultKind().slots;
            }
            int exceptionSlot = next;

            Label loop = new Label();
            Label end = new Label();
            pushInt(mv, 0);
            mv.visitVarInsn(Opcodes.ISTORE, positionSlot);
            mv.visitLabel(loop);
            mv.visitVarInsn(Opcodes.ILOAD, positionSlot);
            mv.visitVarInsn(Opcodes.ILOAD, 1);
            mv.visitJumpInsn(Opcodes.IF_ICMPGE, end);

            for (KernelNode node : nodes) {
                emitNode(mv, node, vectors, firstLeafSlot, positionSlot, nullSlot, valueSlot, exceptionSlot);
            }

            KernelNode root = shape.root();
            Kind resultKind = root.resultKind();
            String builder = Type.getInternalName(resultKind.builderClass);
            String builderDescriptor = Type.getDescriptor(resultKind.builderClass);
            Label append = new Label();
            Label next_ = new Label();
            mv.visitVarInsn(Opcodes.ILOAD, nullSlot[root.index()]);
            mv.visitJumpInsn(Opcodes.IFEQ, append);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, builder, "appendNull", "()" + builderDescriptor, true);
            mv.visitInsn(Opcodes.POP);
            mv.visitJumpInsn(Opcodes.GOTO, next_);
            mv.visitLabel(append);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(resultKind.loadOpcode, valueSlot[root.index()]);
            mv.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builder,
                resultKind.appender,
                "(" + Type.getDescriptor(resultKind.primitive) + ")" + builderDescriptor,
                true
            );
            mv.visitInsn(Opcodes.POP);
            mv.visitLabel(next_);

            mv.visitIincInsn(positionSlot, 1);
            mv.visitJumpInsn(Opcodes.GOTO, loop);
            mv.visitLabel(end);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        /**
         * One node: initialise its null flag and value, check its inputs left to right, then call the kernel
         * inside a handler per warn exception.
         */
        private void emitNode(
            MethodVisitor mv,
            KernelNode node,
            boolean vectors,
            int firstLeafSlot,
            int positionSlot,
            int[] nullSlot,
            int[] valueSlot,
            int exceptionSlot
        ) {
            int n = node.index();
            Kind kind = node.resultKind();
            Label done = new Label();

            pushInt(mv, 0);
            mv.visitVarInsn(Opcodes.ISTORE, nullSlot[n]);
            mv.visitInsn(kind.zeroOpcode);
            mv.visitVarInsn(kind.storeOpcode, valueSlot[n]);

            for (Node input : node.inputs()) {
                if (input instanceof LeafNode leaf) {
                    if (vectors == false) {
                        String block = Type.getInternalName(leaf.kind().blockClass);
                        int slot = firstLeafSlot + leaf.index();
                        Label notNull = new Label();
                        mv.visitVarInsn(Opcodes.ALOAD, slot);
                        mv.visitVarInsn(Opcodes.ILOAD, positionSlot);
                        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, block, "isNull", INT_TO_BOOLEAN, true);
                        mv.visitJumpInsn(Opcodes.IFEQ, notNull);
                        emitNull(mv, nullSlot[n], done);
                        mv.visitLabel(notNull);

                        Label singleValued = new Label();
                        mv.visitVarInsn(Opcodes.ALOAD, slot);
                        mv.visitVarInsn(Opcodes.ILOAD, positionSlot);
                        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, block, "getValueCount", INT_TO_INT, true);
                        pushInt(mv, 1);
                        mv.visitJumpInsn(Opcodes.IF_ICMPEQ, singleValued);
                        mv.visitVarInsn(Opcodes.ALOAD, 0);
                        pushInt(mv, n);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SUPER, "warnMultiValue", WARN_MULTI_VALUE_DESCRIPTOR, false);
                        emitNull(mv, nullSlot[n], done);
                        mv.visitLabel(singleValued);
                    }
                } else if (input instanceof KernelNode inner) {
                    Label notNull = new Label();
                    mv.visitVarInsn(Opcodes.ILOAD, nullSlot[inner.index()]);
                    mv.visitJumpInsn(Opcodes.IFEQ, notNull);
                    emitNull(mv, nullSlot[n], done);
                    mv.visitLabel(notNull);
                }
            }

            EvaluatorKernel kernel = node.kernel();
            Label callStart = new Label();
            Label callEnd = new Label();
            Label[] handlers = new Label[kernel.warnExceptions().size()];
            for (int e = 0; e < handlers.length; e++) {
                handlers[e] = new Label();
                mv.visitTryCatchBlock(callStart, callEnd, handlers[e], Type.getInternalName(kernel.warnExceptions().get(e)));
            }

            int valueIndex = 0;
            int fixedIndex = firstFixedField[n];
            for (EvaluatorKernel.Parameter parameter : kernel.parameters()) {
                if (parameter.fixed()) {
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, internalName, fixedField(fixedIndex), Type.getDescriptor(parameter.type()));
                    fixedIndex++;
                } else {
                    Node input = node.inputs().get(valueIndex++);
                    if (input instanceof LeafNode leaf) {
                        Kind leafKind = leaf.kind();
                        int slot = firstLeafSlot + leaf.index();
                        String getterDescriptor = Type.getMethodDescriptor(Type.getType(leafKind.primitive), Type.INT_TYPE);
                        if (vectors) {
                            mv.visitVarInsn(Opcodes.ALOAD, slot);
                            mv.visitVarInsn(Opcodes.ILOAD, positionSlot);
                            String vector = Type.getInternalName(leafKind.vectorClass);
                            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, vector, leafKind.getter, getterDescriptor, true);
                        } else {
                            String block = Type.getInternalName(leafKind.blockClass);
                            mv.visitVarInsn(Opcodes.ALOAD, slot);
                            mv.visitVarInsn(Opcodes.ALOAD, slot);
                            mv.visitVarInsn(Opcodes.ILOAD, positionSlot);
                            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, block, "getFirstValueIndex", INT_TO_INT, true);
                            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, block, leafKind.getter, getterDescriptor, true);
                        }
                    } else if (input instanceof KernelNode inner) {
                        mv.visitVarInsn(inner.resultKind().loadOpcode, valueSlot[inner.index()]);
                    }
                }
            }
            mv.visitLabel(callStart);
            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(kernel.evaluatorClass()),
                EvaluatorKernel.METHOD_NAME,
                kernel.methodType().toMethodDescriptorString(),
                false
            );
            mv.visitLabel(callEnd);
            mv.visitVarInsn(kind.storeOpcode, valueSlot[n]);
            mv.visitJumpInsn(Opcodes.GOTO, done);

            for (Label handler : handlers) {
                mv.visitLabel(handler);
                mv.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                pushInt(mv, n);
                mv.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SUPER, "warnException", WARN_EXCEPTION_DESCRIPTOR, false);
                emitNull(mv, nullSlot[n], done);
            }
            mv.visitLabel(done);
        }

        /**
         * {@code nullN = true; goto done;}
         */
        private static void emitNull(MethodVisitor mv, int nullSlot, Label done) {
            pushInt(mv, 1);
            mv.visitVarInsn(Opcodes.ISTORE, nullSlot);
            mv.visitJumpInsn(Opcodes.GOTO, done);
        }

        private static void pushInt(MethodVisitor mv, int value) {
            if (value >= -1 && value <= 5) {
                mv.visitInsn(Opcodes.ICONST_0 + value);
            } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.BIPUSH, value);
            } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.SIPUSH, value);
            } else {
                mv.visitLdcInsn(value);
            }
        }
    }
}
