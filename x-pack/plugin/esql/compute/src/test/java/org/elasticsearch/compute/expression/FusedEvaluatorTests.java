/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks the spun {@link FusedEvaluator} against {@link ReferenceEvaluator}, a plain interpreter of the same
 * {@link EvaluatorKernel} contract, over hand-written kernels. The kernels below stand in for generated
 * evaluators: public classes with a {@code public static process} method and a matching descriptor.
 */
public class FusedEvaluatorTests extends ComputeTestCase {
    private static final String MULTI_VALUE = "single-value function encountered multi-value";

    /**
     * Channels of the test page, by type.
     */
    private static final int[] LONG_CHANNELS = { 0, 1, 2 };
    private static final int[] DOUBLE_CHANNELS = { 3, 4 };
    private static final int[] BOOLEAN_CHANNELS = { 5 };
    private static final ElementType[] CHANNEL_TYPES = {
        ElementType.LONG,
        ElementType.LONG,
        ElementType.LONG,
        ElementType.DOUBLE,
        ElementType.DOUBLE,
        ElementType.BOOLEAN };

    public static final class AddLongs {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            AddLongs.class,
            long.class,
            List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.value(long.class)),
            List.of(ArithmeticException.class)
        );

        public static long process(long a, long b) {
            return Math.addExact(a, b);
        }
    }

    public static final class Scale {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            Scale.class,
            long.class,
            List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.fixed(long.class)),
            List.of(ArithmeticException.class)
        );

        public static long process(long a, long factor) {
            return Math.multiplyExact(a, factor);
        }
    }

    public static final class Offset {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            Offset.class,
            long.class,
            List.of(EvaluatorKernel.fixed(Duration.class), EvaluatorKernel.value(long.class)),
            List.of()
        );

        public static long process(Duration offset, long a) {
            return a + offset.toMillis();
        }
    }

    public static final class GreaterThanLongs {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            GreaterThanLongs.class,
            boolean.class,
            List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.value(long.class)),
            List.of()
        );

        public static boolean process(long a, long b) {
            return a > b;
        }
    }

    public static final class Pick {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            Pick.class,
            long.class,
            List.of(EvaluatorKernel.value(boolean.class), EvaluatorKernel.value(long.class), EvaluatorKernel.value(long.class)),
            List.of()
        );

        public static long process(boolean condition, long a, long b) {
            return condition ? a : b;
        }
    }

    public static final class ToDouble {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            ToDouble.class,
            double.class,
            List.of(EvaluatorKernel.value(long.class)),
            List.of()
        );

        public static double process(long a) {
            return a;
        }
    }

    public static final class MulDoubles {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            MulDoubles.class,
            double.class,
            List.of(EvaluatorKernel.value(double.class), EvaluatorKernel.value(double.class)),
            List.of()
        );

        public static double process(double a, double b) {
            return a * b;
        }
    }

    public static final class CheckedToLong {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            CheckedToLong.class,
            long.class,
            List.of(EvaluatorKernel.value(double.class)),
            List.of(IllegalArgumentException.class, ArithmeticException.class)
        );

        public static long process(double a) {
            if (Double.isNaN(a)) {
                throw new IllegalArgumentException("NaN");
            }
            if (Math.abs(a) > Long.MAX_VALUE / 2.0) {
                throw new ArithmeticException("out of range");
            }
            return (long) a;
        }
    }

    private static final class Secret {}

    public static final class WithSecret {
        public static final EvaluatorKernel KERNEL = new EvaluatorKernel(
            WithSecret.class,
            long.class,
            List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.fixed(Secret.class)),
            List.of()
        );

        public static long process(long a, Secret secret) {
            return a;
        }
    }

    private static final List<EvaluatorKernel> KERNELS = List.of(
        AddLongs.KERNEL,
        Scale.KERNEL,
        Offset.KERNEL,
        GreaterThanLongs.KERNEL,
        Pick.KERNEL,
        ToDouble.KERNEL,
        MulDoubles.KERNEL,
        CheckedToLong.KERNEL
    );

    private record Location(int lineNumber, int columnNumber, String viewName, String text) implements WarningSourceLocation {}

    public void testSingleKernelIsNotFused() {
        assertThat(FusedEvaluator.Factory.tryFuse(add(channel(0), channel(1))), equalTo(Optional.empty()));
    }

    public void testNonFusableRootIsNotFused() {
        assertThat(FusedEvaluator.Factory.tryFuse(channel(0)), equalTo(Optional.empty()));
    }

    public void testInaccessibleFixedTypeIsNotFused() {
        TestFactory withSecret = new TestFactory(WithSecret.KERNEL, List.of(add(channel(0), channel(1))), List.of(new Secret()), source(1));
        assertThat(FusedEvaluator.Factory.tryFuse(withSecret), equalTo(Optional.empty()));
        // as an input it stays a leaf, leaving a single kernel to fuse
        assertThat(FusedEvaluator.Factory.tryFuse(add(withSecret, channel(2))), equalTo(Optional.empty()));
    }

    /**
     * Spins directly, so a broken emitter fails with its exception instead of silently falling back to the
     * unfused tree.
     */
    public void testSpins() {
        FusedShape.Builder builder = new FusedShape.Builder();
        FusedShape shape = builder.build(scale(add(channel(0), channel(1)), 2));
        assertThat(shape.nodes(), hasSize(2));
        new FusedEvaluatorSpinner().spin(shape);
    }

    public void testShapeIsSharedAcrossTrees() {
        FusedEvaluator.Factory first = fuse(scale(add(channel(0), channel(1)), 2));
        FusedEvaluator.Factory second = fuse(scale(add(channel(2), channel(0)), 7));
        assertThat(first.shape(), equalTo(second.shape()));
        assertThat(first.constructor(), sameInstance(second.constructor()));
        assertThat(first.shape().nodes(), hasSize(2));
        assertThat(first.shape().leafCount(), equalTo(2));
        assertThat(first.leaves(), equalTo(List.of(channel(0), channel(1))));
        assertThat(first.toString(), equalTo(scale(add(channel(0), channel(1)), 2).toString()));
    }

    /**
     * Mirrors the generated evaluators position by position: a {@code null} input yields {@code null} without a
     * warning, a multi-valued input warns on the node reading it, and a kernel's warn exception warns on the
     * node that threw it. Each node warns with its own location.
     */
    public void testNullsAndWarnings() {
        ExpressionEvaluator.Factory tree = scale(add(channel(0), channel(1)), 2);
        FusedEvaluator.Factory fused = fuse(tree);
        DriverContext context = driverContext();
        LongBlock a = blockFactory().newLongBlockBuilder(6)
            .appendLong(1)
            .appendNull()
            .beginPositionEntry()
            .appendLong(2)
            .appendLong(3)
            .endPositionEntry()
            .appendLong(Long.MAX_VALUE)
            .appendLong(1L << 61)
            .appendLong(1)
            .build();
        LongBlock b = blockFactory().newLongBlockBuilder(6)
            .appendLong(1)
            .appendLong(1)
            .appendLong(1)
            .appendLong(1)
            .appendLong(1L << 61)
            .beginPositionEntry()
            .appendLong(1)
            .appendLong(2)
            .endPositionEntry()
            .build();
        Page page = new Page(a, b);
        try (ExpressionEvaluator evaluator = fused.get(context); Block result = evaluator.eval(page)) {
            assertThat(evaluator, instanceOf(FusedEvaluator.class));
            assertThat(evaluator.toString(), equalTo(tree.toString()));
            try (
                LongBlock expected = blockFactory().newLongBlockBuilder(6)
                    .appendLong(4)
                    .appendNull()
                    .appendNull()
                    .appendNull()
                    .appendNull()
                    .appendNull()
                    .build()
            ) {
                assertThat(result, equalTo(expected));
            }
        } finally {
            page.releaseBlocks();
        }
        context.finish();
        assertThat(
            context.warnings(),
            containsInAnyOrder(
                "Line 1:1: evaluation of [source 1] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: " + MULTI_VALUE,
                "Line 1:1: java.lang.ArithmeticException: long overflow",
                "Line 2:1: evaluation of [source 2] failed, treating result as null. Only first 20 failures recorded.",
                "Line 2:1: java.lang.ArithmeticException: long overflow"
            )
        );
    }

    /**
     * Inputs are checked left to right and a {@code null} stops the node, so a multi-value after it doesn't warn.
     */
    public void testNullBeforeMultiValueDoesNotWarn() {
        FusedEvaluator.Factory fused = fuse(scale(add(channel(0), channel(1)), 2));
        DriverContext context = driverContext();
        LongBlock a = blockFactory().newLongBlockBuilder(1).appendNull().build();
        LongBlock b = blockFactory().newLongBlockBuilder(1).beginPositionEntry().appendLong(1).appendLong(2).endPositionEntry().build();
        Page page = new Page(a, b);
        try (ExpressionEvaluator evaluator = fused.get(context); Block result = evaluator.eval(page)) {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            page.releaseBlocks();
        }
        context.finish();
        assertThat(context.warnings(), empty());
    }

    public void testFixedReferenceValue() {
        Duration offset = Duration.ofHours(randomIntBetween(1, 100));
        TestFactory tree = new TestFactory(Offset.KERNEL, List.of(add(channel(0), channel(1))), List.of(offset), source(2));
        FusedEvaluator.Factory fused = fuse(tree);
        DriverContext context = driverContext();
        long x = randomLongBetween(-1_000_000, 1_000_000);
        long y = randomLongBetween(-1_000_000, 1_000_000);
        Page page = new Page(blockFactory().newConstantLongBlockWith(x, 3), blockFactory().newConstantLongBlockWith(y, 3));
        try (ExpressionEvaluator evaluator = fused.get(context); LongBlock result = (LongBlock) evaluator.eval(page)) {
            for (int p = 0; p < 3; p++) {
                assertThat(result.getLong(p), equalTo(x + y + offset.toMillis()));
            }
        } finally {
            page.releaseBlocks();
        }
        context.finish();
        assertThat(context.warnings(), empty());
    }

    /**
     * Random trees of the kernels above over random pages, with and without nulls and multi-values, produce
     * the same blocks and the same warnings fused as interpreted.
     */
    public void testRandomTreesMatchReference() {
        for (int i = 0; i < 100; i++) {
            ExpressionEvaluator.Factory tree = randomTree(
                randomFrom(FusedShape.Kind.LONG, FusedShape.Kind.DOUBLE, FusedShape.Kind.BOOLEAN),
                4
            );
            Optional<FusedEvaluator.Factory> fused = FusedEvaluator.Factory.tryFuse(tree);
            if (kernelCount(tree) < 2) {
                assertThat(tree.toString(), fused, equalTo(Optional.empty()));
                continue;
            }
            assertThat(tree.toString(), fused.isPresent(), equalTo(true));
            int positionCount = randomIntBetween(0, 200);
            boolean allVectors = randomBoolean();
            Block[] blocks = new Block[CHANNEL_TYPES.length];
            for (int c = 0; c < blocks.length; c++) {
                blocks[c] = RandomBlock.randomBlock(
                    blockFactory(),
                    CHANNEL_TYPES[c],
                    positionCount,
                    allVectors == false && randomBoolean(),
                    1,
                    allVectors ? 1 : randomIntBetween(1, 3),
                    0,
                    0
                ).block();
            }
            Page page = new Page(positionCount, blocks);
            try {
                assertSameResult(tree, fused.get(), page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private void assertSameResult(ExpressionEvaluator.Factory reference, ExpressionEvaluator.Factory fused, Page page) {
        DriverContext referenceContext = driverContext();
        DriverContext fusedContext = driverContext();
        try (
            ExpressionEvaluator referenceEvaluator = reference.get(referenceContext);
            ExpressionEvaluator fusedEvaluator = fused.get(fusedContext);
            Block expected = referenceEvaluator.eval(page);
            Block actual = fusedEvaluator.eval(page)
        ) {
            assertThat(reference.toString(), actual, equalTo(expected));
        }
        referenceContext.finish();
        fusedContext.finish();
        Set<String> expectedWarnings = withoutArithmeticMessages(referenceContext.warnings());
        Set<String> actualWarnings = withoutArithmeticMessages(fusedContext.warnings());
        assertThat(reference.toString(), actualWarnings, equalTo(expectedWarnings));
    }

    /**
     * Once a compiled method has thrown enough overflow exceptions HotSpot throws a preallocated
     * {@link ArithmeticException} without a message ({@code -XX:+OmitStackTraceInFastThrow}), so the hot side of
     * the comparison can report {@code ArithmeticException: null} where the cold side reports
     * {@code ArithmeticException: long overflow}. The message is the JVM's to drop either way;
     * {@link #testNullsAndWarnings} checks it is passed through when present.
     */
    private static Set<String> withoutArithmeticMessages(List<String> warnings) {
        Set<String> normalized = new HashSet<>();
        for (String warning : warnings) {
            int message = warning.indexOf("java.lang.ArithmeticException: ");
            normalized.add(message < 0 ? warning : warning.substring(0, message + "java.lang.ArithmeticException".length()));
        }
        return normalized;
    }

    private DriverContext driverContext() {
        return new DriverContext(nonBreakingBigArrays(), blockFactory(), null);
    }

    private static int kernelCount(ExpressionEvaluator.Factory factory) {
        if (factory instanceof TestFactory test) {
            int count = 1;
            for (ExpressionEvaluator.Factory input : test.inputs()) {
                count += kernelCount(input);
            }
            return count;
        }
        return 0;
    }

    private int nextSource = 1;

    private ExpressionEvaluator.Factory randomTree(FusedShape.Kind kind, int depth) {
        if (depth == 0 || randomInt(4) == 0) {
            return channel(randomChannel(kind));
        }
        List<EvaluatorKernel> candidates = new ArrayList<>();
        for (EvaluatorKernel kernel : KERNELS) {
            if (kernel.resultType() == kind.primitive) {
                candidates.add(kernel);
            }
        }
        EvaluatorKernel kernel = randomFrom(candidates);
        List<ExpressionEvaluator.Factory> inputs = new ArrayList<>();
        List<Object> fixed = new ArrayList<>();
        for (EvaluatorKernel.Parameter parameter : kernel.parameters()) {
            if (parameter.fixed()) {
                fixed.add(randomFixed(parameter.type()));
            } else {
                inputs.add(randomTree(FusedShape.Kind.of(parameter.type()), depth - 1));
            }
        }
        return new TestFactory(kernel, inputs, fixed, source(nextSource++));
    }

    private static Object randomFixed(Class<?> type) {
        if (type == long.class) {
            return randomLongBetween(-10, 10);
        }
        if (type == Duration.class) {
            return Duration.ofMillis(randomLongBetween(-10_000, 10_000));
        }
        throw new AssertionError("no random value for " + type);
    }

    private static int randomChannel(FusedShape.Kind kind) {
        return switch (kind) {
            case LONG -> randomFrom(LONG_CHANNELS);
            case DOUBLE -> randomFrom(DOUBLE_CHANNELS);
            case BOOLEAN -> randomFrom(BOOLEAN_CHANNELS);
            case INT, FLOAT -> throw new AssertionError("no channel of " + kind);
        };
    }

    private static int randomFrom(int[] values) {
        return values[randomInt(values.length - 1)];
    }

    private static FusedEvaluator.Factory fuse(ExpressionEvaluator.Factory tree) {
        Optional<FusedEvaluator.Factory> fused = FusedEvaluator.Factory.tryFuse(tree);
        assertThat(tree.toString(), fused.isPresent(), equalTo(true));
        return fused.get();
    }

    private static ExpressionEvaluator.Factory channel(int channel) {
        return new LoadFromPageEvaluator.Factory(channel);
    }

    private static TestFactory add(ExpressionEvaluator.Factory a, ExpressionEvaluator.Factory b) {
        return new TestFactory(AddLongs.KERNEL, List.of(a, b), List.of(), source(1));
    }

    private static TestFactory scale(ExpressionEvaluator.Factory a, long factor) {
        return new TestFactory(Scale.KERNEL, List.of(a), List.of(factor), source(2));
    }

    private static Location source(int line) {
        return new Location(line, 1, null, "source " + line);
    }

    /**
     * A fusable factory over one of the test kernels whose evaluator is the {@link ReferenceEvaluator}.
     */
    private record TestFactory(EvaluatorKernel kernel, List<ExpressionEvaluator.Factory> inputs, List<Object> fixedValues, Location source)
        implements
            FusableEvaluatorFactory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            List<ExpressionEvaluator> evaluators = new ArrayList<>(inputs.size());
            for (ExpressionEvaluator.Factory input : inputs) {
                evaluators.add(input.get(context));
            }
            return new ReferenceEvaluator(kernel, evaluators, fixedValues, source, context);
        }

        @Override
        public String toString() {
            return kernel.evaluatorClass().getSimpleName() + inputs + fixedValues;
        }
    }

    /**
     * Applies a kernel the slow way, through its {@link EvaluatorKernel#methodHandle()} with boxed values,
     * following the generated evaluators' per-position contract. This is the oracle the spun code is
     * checked against.
     */
    private static final class ReferenceEvaluator implements ExpressionEvaluator {
        private final EvaluatorKernel kernel;
        private final MethodHandle handle;
        private final List<ExpressionEvaluator> inputs;
        private final List<Object> fixedValues;
        private final WarningSourceLocation source;
        private final DriverContext context;
        private Warnings warnings;

        ReferenceEvaluator(
            EvaluatorKernel kernel,
            List<ExpressionEvaluator> inputs,
            List<Object> fixedValues,
            WarningSourceLocation source,
            DriverContext context
        ) {
            this.kernel = kernel;
            this.handle = kernel.methodHandle();
            this.inputs = inputs;
            this.fixedValues = fixedValues;
            this.source = source;
            this.context = context;
        }

        @Override
        public Block eval(Page page) {
            Block[] blocks = new Block[inputs.size()];
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = inputs.get(i).eval(page);
                }
                int positionCount = page.getPositionCount();
                ElementType resultType = FusedShape.Kind.of(kernel.resultType()).elementType;
                try (Block.Builder result = resultType.newBlockBuilder(positionCount, context.blockFactory())) {
                    positions: for (int p = 0; p < positionCount; p++) {
                        Object[] args = new Object[kernel.parameters().size()];
                        int input = 0;
                        int fixed = 0;
                        for (int i = 0; i < args.length; i++) {
                            if (kernel.parameters().get(i).fixed()) {
                                args[i] = fixedValues.get(fixed++);
                                continue;
                            }
                            Block block = blocks[input++];
                            if (block.isNull(p)) {
                                result.appendNull();
                                continue positions;
                            }
                            if (block.getValueCount(p) != 1) {
                                warnings().registerException(new IllegalArgumentException(MULTI_VALUE));
                                result.appendNull();
                                continue positions;
                            }
                            args[i] = read(block, block.getFirstValueIndex(p));
                        }
                        Object value;
                        try {
                            value = handle.invokeWithArguments(args);
                        } catch (Exception e) {
                            if (kernel.warnExceptions().stream().anyMatch(w -> w.isInstance(e))) {
                                warnings().registerException(e);
                                result.appendNull();
                                continue;
                            }
                            throw new AssertionError(e);
                        } catch (Throwable t) {
                            throw new AssertionError(t);
                        }
                        append(result, value);
                    }
                    return result.build();
                }
            } finally {
                Releasables.closeExpectNoException(blocks);
            }
        }

        private static Object read(Block block, int index) {
            if (block instanceof LongBlock b) {
                return b.getLong(index);
            }
            if (block instanceof IntBlock b) {
                return b.getInt(index);
            }
            if (block instanceof DoubleBlock b) {
                return b.getDouble(index);
            }
            if (block instanceof BooleanBlock b) {
                return b.getBoolean(index);
            }
            throw new AssertionError("unsupported " + block);
        }

        private static void append(Block.Builder builder, Object value) {
            if (builder instanceof LongBlock.Builder b) {
                b.appendLong((Long) value);
            } else if (builder instanceof IntBlock.Builder b) {
                b.appendInt((Integer) value);
            } else if (builder instanceof DoubleBlock.Builder b) {
                b.appendDouble((Double) value);
            } else if (builder instanceof BooleanBlock.Builder b) {
                b.appendBoolean((Boolean) value);
            } else {
                throw new AssertionError("unsupported " + builder);
            }
        }

        private Warnings warnings() {
            if (warnings == null) {
                warnings = context.createWarnings(source);
            }
            return warnings;
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(inputs);
        }
    }
}
