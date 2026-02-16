/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.ann.RuntimeAggregator;
import org.elasticsearch.compute.ann.RuntimeIntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * Tests for {@link AggregatorSpec} and {@link RuntimeAggregatorGenerator}.
 */
public class RuntimeAggregatorGeneratorTests extends ESTestCase {

    /**
     * Test aggregator class for sum of longs.
     * Uses only long values to avoid ambiguity with multiple combine methods.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "sum", type = "LONG"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestSumIntAggregator {
        public static long init() {
            return 0;
        }

        public static long combine(long current, long v) {
            return Math.addExact(current, v);
        }
    }

    /**
     * Test aggregator class for summing BytesRef lengths.
     * Tests BytesRef input handling with primitive state.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "lengthSum", type = "LONG"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestLengthSumAggregator {
        public static long init() {
            return 0;
        }

        public static long combine(long current, BytesRef v) {
            return Math.addExact(current, v.length);
        }

        public static long combine(long current, long v) {
            return Math.addExact(current, v);
        }
    }

    /**
     * Test aggregator class with warnExceptions for overflow protection.
     * Tests the warnExceptions support in RuntimeAggregatorGenerator.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "sum", type = "LONG"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN"),
            @RuntimeIntermediateState(name = "failed", type = "BOOLEAN") },
        warnExceptions = { ArithmeticException.class }
    )
    public static class TestSafeSumAggregator {
        public static long init() {
            return 0;
        }

        public static long combine(long current, long v) {
            return Math.addExact(current, v);
        }
    }

    /**
     * Test aggregator class for sum of doubles.
     * Tests double state type support.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "sum", type = "DOUBLE"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestDoubleSumAggregator {
        public static double init() {
            return 0.0;
        }

        public static double combine(double current, double v) {
            return current + v;
        }
    }

    /**
     * Test aggregator class for summing integers with int state.
     * Tests int state type support.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "sum", type = "INT"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestIntSumAggregator {
        public static int init() {
            return 0;
        }

        public static int combine(int current, int v) {
            return current + v;
        }
    }

    /**
     * Test aggregator class with first method for non-primitive state.
     * Tests first method detection in AggregatorSpec.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "max", type = "BYTES_REF"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestMaxBytesAggregator {
        public static TestBytesState initSingle() {
            return new TestBytesState();
        }

        public static void first(TestBytesState state, BytesRef value) {
            state.setValue(value);
        }

        public static void combine(TestBytesState state, BytesRef value) {
            if (state.getValue() == null || value.compareTo(state.getValue()) > 0) {
                state.setValue(value);
            }
        }
    }

    /**
     * Simple test state class for BytesRef aggregation.
     */
    public static class TestBytesState {
        private BytesRef value;
        private boolean seen;

        public BytesRef getValue() {
            return value;
        }

        public void setValue(BytesRef value) {
            this.value = BytesRef.deepCopyOf(value);
            this.seen = true;
        }

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }
    }

    /**
     * Test aggregator class with custom state implementing AggregatorState.
     * This is the pattern used by MaxBytesRefAggregator.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "max", type = "BYTES_REF"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
    )
    public static class TestMaxBytesRefAggregator {
        private static boolean isBetter(BytesRef value, BytesRef otherValue) {
            return value.compareTo(otherValue) > 0;
        }

        public static SingleState initSingle(DriverContext driverContext) {
            return new SingleState(driverContext.breaker());
        }

        public static void combine(SingleState state, BytesRef value) {
            state.add(value);
        }

        public static void combineIntermediate(SingleState state, BytesRef value, boolean seen) {
            if (seen) {
                combine(state, value);
            }
        }

        public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
            return state.toBlock(driverContext);
        }

        public static class SingleState implements AggregatorState {
            private final BreakingBytesRefBuilder internalState;
            private boolean seen;

            public SingleState(CircuitBreaker breaker) {
                this.internalState = new BreakingBytesRefBuilder(breaker, "test_max_bytes_ref_aggregator");
                this.seen = false;
            }

            public void add(BytesRef value) {
                if (seen == false || isBetter(value, internalState.bytesRefView())) {
                    seen = true;
                    internalState.grow(value.length);
                    internalState.setLength(value.length);
                    System.arraycopy(value.bytes, value.offset, internalState.bytes(), 0, value.length);
                }
            }

            public boolean seen() {
                return seen;
            }

            public void seen(boolean seen) {
                this.seen = seen;
            }

            @Override
            public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
                blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
                blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
            }

            public Block toBlock(DriverContext driverContext) {
                if (seen == false) {
                    return driverContext.blockFactory().newConstantNullBlock(1);
                }
                return driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
            }

            @Override
            public void close() {
                Releasables.close(internalState);
            }
        }
    }

    public void testAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);

        assertEquals(TestSumIntAggregator.class, spec.declaringClass());
        assertEquals("init", spec.initMethod().getName());
        assertEquals("combine", spec.combineMethod().getName());
        assertEquals(long.class, spec.stateType());
        assertTrue(spec.isPrimitiveState());
        assertEquals(2, spec.intermediateState().size());
        assertEquals("sum", spec.intermediateState().get(0).name());
        assertEquals("LONG", spec.intermediateState().get(0).elementType());
        assertEquals("seen", spec.intermediateState().get(1).name());
        assertEquals("BOOLEAN", spec.intermediateState().get(1).elementType());

        // Primitive state should not have first method
        assertFalse("Primitive state should not have first method", spec.hasFirstMethod());
        assertNull("First method should be null for primitive state", spec.firstMethod());
    }

    public void testFirstMethodDetection() {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesAggregator.class);

        assertEquals(TestMaxBytesAggregator.class, spec.declaringClass());
        assertEquals("initSingle", spec.initMethod().getName());
        assertEquals("combine", spec.combineMethod().getName());
        assertEquals(TestBytesState.class, spec.stateType());
        assertFalse("Non-primitive state", spec.isPrimitiveState());

        // Non-primitive state with first method should be detected
        assertTrue("Should have first method", spec.hasFirstMethod());
        assertNotNull("First method should not be null", spec.firstMethod());
        assertEquals("first", spec.firstMethod().getName());

        // Verify first method has same parameters as combine
        assertEquals(spec.combineMethod().getParameterCount(), spec.firstMethod().getParameterCount());
    }

    public void testFirstMethodNotDetectedForPrimitiveState() {
        // For primitive state types, first method should not be detected even if present
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        assertFalse("Primitive state should not have first method", spec.hasFirstMethod());
    }

    public void testAggregatorSpecNaming() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);

        assertEquals("TestSumIntAggregatorFunction", spec.aggregatorSimpleName());
        // Generated classes are now in the same package as the declaring class
        assertEquals("org.elasticsearch.compute.runtime.TestSumIntAggregatorFunction", spec.aggregatorClassName());
        assertEquals("TestSumIntGroupingAggregatorFunction", spec.groupingAggregatorSimpleName());
        assertEquals("org.elasticsearch.compute.runtime.TestSumIntGroupingAggregatorFunction", spec.groupingAggregatorClassName());
    }

    public void testAggregatorSpecStateWrapper() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);

        assertEquals("org.elasticsearch.compute.aggregation.LongState", spec.stateWrapperClassName());
        assertEquals("org.elasticsearch.compute.aggregation.LongArrayState", spec.arrayStateWrapperClassName());
    }

    public void testAggregatorSpecValueType() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);

        assertEquals(1, spec.combineParameterTypes().size());
        Class<?> valueType = spec.valueType();
        assertEquals("Value type should be long", long.class, valueType);
    }

    public void testAggregatorSpecIntermediateBlockCount() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);

        assertEquals(2, spec.intermediateBlockCount());
    }

    public void testBytecodeGeneration() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        // Generate bytecode (don't load it yet)
        byte[] bytecode = generator.generateAggregatorBytecode(spec);
        assertNotNull("Bytecode should be generated", bytecode);
        assertTrue("Bytecode should have content", bytecode.length > 0);
    }

    public void testAggregatorClassGeneration() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        assertNotNull(aggregatorClass);
        assertTrue(AggregatorFunction.class.isAssignableFrom(aggregatorClass));

        // Verify the class has the expected methods
        assertHasMethod(aggregatorClass, "create", true);
        assertHasMethod(aggregatorClass, "intermediateStateDesc", true);
        assertHasMethod(aggregatorClass, "intermediateBlockCount", false);
        assertHasMethod(aggregatorClass, "addRawInput", false);
        assertHasMethod(aggregatorClass, "addIntermediateInput", false);
        assertHasMethod(aggregatorClass, "evaluateIntermediate", false);
        assertHasMethod(aggregatorClass, "evaluateFinal", false);
        assertHasMethod(aggregatorClass, "toString", false);
        assertHasMethod(aggregatorClass, "close", false);
    }

    public void testGroupingAggregatorClassGeneration() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> groupingClass = generator.getOrGenerateGroupingAggregator(spec);

        assertNotNull(groupingClass);
        assertTrue(GroupingAggregatorFunction.class.isAssignableFrom(groupingClass));

        // Verify the class has the expected methods
        assertHasMethod(groupingClass, "create", true);
        assertHasMethod(groupingClass, "intermediateStateDesc", true);
        assertHasMethod(groupingClass, "intermediateBlockCount", false);
        assertHasMethod(groupingClass, "prepareProcessRawInputPage", false);
        assertHasMethod(groupingClass, "selectedMayContainUnseenGroups", false);
        assertHasMethod(groupingClass, "evaluateIntermediate", false);
        assertHasMethod(groupingClass, "evaluateFinal", false);
        assertHasMethod(groupingClass, "toString", false);
        assertHasMethod(groupingClass, "close", false);
    }

    public void testCaching() {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> first = generator.getOrGenerateAggregator(spec);
        Class<? extends AggregatorFunction> second = generator.getOrGenerateAggregator(spec);

        assertSame("Should return cached class", first, second);
    }

    public void testAggregatorFunctionCreation() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        assertNotNull("Aggregator should be created", aggregator);

        // Verify intermediateStateDesc
        Method intermediateStateDescMethod = aggregatorClass.getMethod("intermediateStateDesc");
        @SuppressWarnings("unchecked")
        List<IntermediateStateDesc> stateDesc = (List<IntermediateStateDesc>) intermediateStateDescMethod.invoke(null);
        assertEquals(2, stateDesc.size());
        assertEquals("sum", stateDesc.get(0).name());
        assertEquals(ElementType.LONG, stateDesc.get(0).type());
        assertEquals("seen", stateDesc.get(1).name());
        assertEquals(ElementType.BOOLEAN, stateDesc.get(1).type());

        // Verify intermediateBlockCount
        assertEquals(2, aggregator.intermediateBlockCount());

        // Clean up
        aggregator.close();
    }

    public void testAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some long values: [1, 2, 3, 4, 5]
        LongBlock.Builder builder = blockFactory.newLongBlockBuilder(5);
        builder.appendLong(1);
        builder.appendLong(2);
        builder.appendLong(3);
        builder.appendLong(4);
        builder.appendLong(5);
        LongBlock longBlock = builder.build();
        Page page = new Page(longBlock);

        // Create a mask that includes all positions (all true = no filtering)
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 5);

        // Add raw input
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a LongBlock with value 15 (1+2+3+4+5)
        assertTrue("Result should be a LongBlock", results[0] instanceof LongBlock);
        LongBlock resultBlock = (LongBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Sum should be 15", 15L, resultBlock.getLong(0));

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testGroupingAggregatorFunctionCreation() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> aggregatorClass = generator.getOrGenerateGroupingAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", List.class, DriverContext.class);
        List<Integer> channels = List.of(0);
        GroupingAggregatorFunction aggregator = (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);

        assertNotNull("Aggregator should be created", aggregator);
        assertEquals("Intermediate block count should be 2", 2, aggregator.intermediateBlockCount());

        // Clean up
        aggregator.close();
    }

    public void testGroupingAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSumIntAggregator.class);
        RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> aggregatorClass = generator.getOrGenerateGroupingAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", List.class, DriverContext.class);
        List<Integer> channels = List.of(0);
        GroupingAggregatorFunction aggregator = (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);

        // Create a page with some long values: [10, 20, 30, 40, 50]
        LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(5);
        valueBuilder.appendLong(10);
        valueBuilder.appendLong(20);
        valueBuilder.appendLong(30);
        valueBuilder.appendLong(40);
        valueBuilder.appendLong(50);
        LongBlock valueBlock = valueBuilder.build();
        Page page = new Page(valueBlock);

        // Create group IDs: all values go to group 0
        IntVector groupIds = blockFactory.newConstantIntVector(0, 5);

        // Prepare and add raw input
        SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
        GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessRawInputPage(seenGroupIds, page);
        assertNotNull("AddInput should not be null", addInput);

        // Add input for all positions
        addInput.add(0, groupIds);
        addInput.close();

        // Evaluate final result
        IntVector selected = blockFactory.newConstantIntVector(0, 1);
        Block[] results = new Block[1];
        GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
        aggregator.evaluateFinal(results, 0, selected, ctx);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a LongBlock with value 150 (10+20+30+40+50)
        assertTrue("Result should be a LongBlock", results[0] instanceof LongBlock);
        LongBlock resultBlock = (LongBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Sum should be 150", 150L, resultBlock.getLong(0));

        // Clean up
        results[0].close();
        selected.close();
        groupIds.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testBytesRefAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestLengthSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some BytesRef values: ["hello", "world", "test"]
        // lengths: 5 + 5 + 4 = 14
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3);
        builder.appendBytesRef(new BytesRef("hello"));
        builder.appendBytesRef(new BytesRef("world"));
        builder.appendBytesRef(new BytesRef("test"));
        BytesRefBlock bytesRefBlock = builder.build();
        Page page = new Page(bytesRefBlock);

        // Create a mask that includes all positions (all true = no filtering)
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 3);

        // Add raw input
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a LongBlock with value 14 (5+5+4)
        assertTrue("Result should be a LongBlock", results[0] instanceof LongBlock);
        LongBlock resultBlock = (LongBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Length sum should be 14", 14L, resultBlock.getLong(0));

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testBytesRefAggregatorSpecValueType() {
        AggregatorSpec spec = AggregatorSpec.from(TestLengthSumAggregator.class);

        assertEquals(1, spec.combineParameterTypes().size());
        Class<?> valueType = spec.valueType();
        assertEquals("Value type should be BytesRef", BytesRef.class, valueType);
    }

    public void testGroupingBytesRefAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestLengthSumAggregator.class);
        RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> aggregatorClass = generator.getOrGenerateGroupingAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", List.class, DriverContext.class);
        List<Integer> channels = List.of(0);
        GroupingAggregatorFunction aggregator = (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);

        // Create a page with some BytesRef values: ["hello", "world", "test", "foo", "bar"]
        // lengths: 5, 5, 4, 3, 3
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(5);
        builder.appendBytesRef(new BytesRef("hello"));
        builder.appendBytesRef(new BytesRef("world"));
        builder.appendBytesRef(new BytesRef("test"));
        builder.appendBytesRef(new BytesRef("foo"));
        builder.appendBytesRef(new BytesRef("bar"));
        BytesRefBlock valueBlock = builder.build();
        Page page = new Page(valueBlock);

        // Create group IDs: [0, 0, 1, 1, 0]
        // Group 0: "hello" (5) + "world" (5) + "bar" (3) = 13
        // Group 1: "test" (4) + "foo" (3) = 7
        IntVector.Builder groupBuilder = blockFactory.newIntVectorBuilder(5);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(0);
        IntVector groupIds = groupBuilder.build();

        // Prepare and add raw input
        SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
        GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessRawInputPage(seenGroupIds, page);
        assertNotNull("AddInput should not be null", addInput);

        // Add input for all positions
        addInput.add(0, groupIds);
        addInput.close();

        // Evaluate final result for both groups
        IntVector.Builder selectedBuilder = blockFactory.newIntVectorBuilder(2);
        selectedBuilder.appendInt(0);
        selectedBuilder.appendInt(1);
        IntVector selected = selectedBuilder.build();
        Block[] results = new Block[1];
        GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
        aggregator.evaluateFinal(results, 0, selected, ctx);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a LongBlock with values [13, 7]
        assertTrue("Result should be a LongBlock", results[0] instanceof LongBlock);
        LongBlock resultBlock = (LongBlock) results[0];
        assertEquals("Result should have 2 positions", 2, resultBlock.getPositionCount());
        assertEquals("Group 0 length sum should be 13", 13L, resultBlock.getLong(0));
        assertEquals("Group 1 length sum should be 7", 7L, resultBlock.getLong(1));

        // Clean up
        results[0].close();
        selected.close();
        groupIds.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testWarnExceptionsAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestSafeSumAggregator.class);

        assertTrue("Should have warn exceptions", spec.hasWarnExceptions());
        assertEquals(1, spec.warnExceptions().size());
        assertEquals(ArithmeticException.class, spec.warnExceptions().get(0));

        // With warnExceptions, state wrapper should be FallibleState
        assertEquals("org.elasticsearch.compute.aggregation.LongFallibleState", spec.stateWrapperClassName());
        assertEquals("org.elasticsearch.compute.aggregation.LongFallibleArrayState", spec.arrayStateWrapperClassName());

        // Intermediate state should have 3 elements: sum, seen, failed
        assertEquals(3, spec.intermediateState().size());
        assertEquals("sum", spec.intermediateState().get(0).name());
        assertEquals("seen", spec.intermediateState().get(1).name());
        assertEquals("failed", spec.intermediateState().get(2).name());
    }

    @SuppressForbidden(reason = "Test needs to verify generated class has expected private field")
    public void testWarnExceptionsAggregatorClassGeneration() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSafeSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        assertNotNull(aggregatorClass);
        assertTrue(AggregatorFunction.class.isAssignableFrom(aggregatorClass));

        // Verify the class has the expected public methods
        assertHasMethod(aggregatorClass, "create", true);
        assertHasMethod(aggregatorClass, "intermediateStateDesc", true);
        assertHasMethod(aggregatorClass, "addRawInput", false);

        // Verify the warnings field exists (private field)
        try {
            aggregatorClass.getDeclaredField("warnings");
        } catch (NoSuchFieldException e) {
            fail("Class should have a warnings field");
        }
    }

    public void testWarnExceptionsAggregatorNormalOperation() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestSafeSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some long values: [1, 2, 3, 4, 5]
        LongBlock.Builder builder = blockFactory.newLongBlockBuilder(5);
        builder.appendLong(1);
        builder.appendLong(2);
        builder.appendLong(3);
        builder.appendLong(4);
        builder.appendLong(5);
        LongBlock longBlock = builder.build();
        Page page = new Page(longBlock);

        // Create a mask that includes all positions
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 5);

        // Add raw input - should work normally without overflow
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a LongBlock with value 15 (1+2+3+4+5)
        assertTrue("Result should be a LongBlock", results[0] instanceof LongBlock);
        LongBlock resultBlock = (LongBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Sum should be 15", 15L, resultBlock.getLong(0));

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testDoubleStateAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestDoubleSumAggregator.class);

        assertEquals(TestDoubleSumAggregator.class, spec.declaringClass());
        assertEquals("init", spec.initMethod().getName());
        assertEquals("combine", spec.combineMethod().getName());
        assertEquals(double.class, spec.stateType());
        assertTrue(spec.isPrimitiveState());
        assertEquals(2, spec.intermediateState().size());
        assertEquals("sum", spec.intermediateState().get(0).name());
        assertEquals("DOUBLE", spec.intermediateState().get(0).elementType());
        assertEquals("seen", spec.intermediateState().get(1).name());
        assertEquals("BOOLEAN", spec.intermediateState().get(1).elementType());

        // Verify state wrapper class names
        assertEquals("org.elasticsearch.compute.aggregation.DoubleState", spec.stateWrapperClassName());
        assertEquals("org.elasticsearch.compute.aggregation.DoubleArrayState", spec.arrayStateWrapperClassName());
    }

    public void testDoubleStateAggregatorClassGeneration() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestDoubleSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        assertNotNull(aggregatorClass);
        assertTrue(AggregatorFunction.class.isAssignableFrom(aggregatorClass));

        // Verify the class has the expected methods
        assertHasMethod(aggregatorClass, "create", true);
        assertHasMethod(aggregatorClass, "intermediateStateDesc", true);
        assertHasMethod(aggregatorClass, "addRawInput", false);
    }

    public void testDoubleStateAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestDoubleSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some double values: [1.5, 2.5, 3.0, 4.0, 5.0]
        DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(5);
        builder.appendDouble(1.5);
        builder.appendDouble(2.5);
        builder.appendDouble(3.0);
        builder.appendDouble(4.0);
        builder.appendDouble(5.0);
        DoubleBlock doubleBlock = builder.build();
        Page page = new Page(doubleBlock);

        // Create a mask that includes all positions
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 5);

        // Add raw input
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a DoubleBlock with value 16.0 (1.5+2.5+3.0+4.0+5.0)
        assertTrue("Result should be a DoubleBlock", results[0] instanceof DoubleBlock);
        DoubleBlock resultBlock = (DoubleBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Sum should be 16.0", 16.0, resultBlock.getDouble(0), 0.0001);

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testIntStateAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestIntSumAggregator.class);

        assertEquals(TestIntSumAggregator.class, spec.declaringClass());
        assertEquals("init", spec.initMethod().getName());
        assertEquals("combine", spec.combineMethod().getName());
        assertEquals(int.class, spec.stateType());
        assertTrue(spec.isPrimitiveState());
        assertEquals(2, spec.intermediateState().size());
        assertEquals("sum", spec.intermediateState().get(0).name());
        assertEquals("INT", spec.intermediateState().get(0).elementType());
        assertEquals("seen", spec.intermediateState().get(1).name());
        assertEquals("BOOLEAN", spec.intermediateState().get(1).elementType());

        // Verify state wrapper class names
        assertEquals("org.elasticsearch.compute.aggregation.IntState", spec.stateWrapperClassName());
        assertEquals("org.elasticsearch.compute.aggregation.IntArrayState", spec.arrayStateWrapperClassName());
    }

    public void testIntStateAggregatorClassGeneration() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestIntSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        assertNotNull(aggregatorClass);
        assertTrue(AggregatorFunction.class.isAssignableFrom(aggregatorClass));

        // Verify the class has the expected methods
        assertHasMethod(aggregatorClass, "create", true);
        assertHasMethod(aggregatorClass, "intermediateStateDesc", true);
        assertHasMethod(aggregatorClass, "addRawInput", false);
    }

    public void testIntStateAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestIntSumAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some int values: [1, 2, 3, 4, 5]
        IntBlock.Builder builder = blockFactory.newIntBlockBuilder(5);
        builder.appendInt(1);
        builder.appendInt(2);
        builder.appendInt(3);
        builder.appendInt(4);
        builder.appendInt(5);
        IntBlock intBlock = builder.build();
        Page page = new Page(intBlock);

        // Create a mask that includes all positions
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 5);

        // Add raw input
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is an IntBlock with value 15 (1+2+3+4+5)
        assertTrue("Result should be an IntBlock", results[0] instanceof IntBlock);
        IntBlock resultBlock = (IntBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        assertEquals("Sum should be 15", 15, resultBlock.getInt(0));

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testCustomStateAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefAggregator.class);

        assertEquals(TestMaxBytesRefAggregator.class, spec.declaringClass());
        assertEquals("initSingle", spec.initMethod().getName());
        assertEquals("combine", spec.combineMethod().getName());
        assertEquals(TestMaxBytesRefAggregator.SingleState.class, spec.stateType());
        assertFalse("Should not be primitive state", spec.isPrimitiveState());
        assertTrue("Should be custom state", spec.isCustomState());
        assertTrue("Init should require DriverContext", spec.initRequiresDriverContext());

        // Custom state should use the state class directly
        assertEquals(TestMaxBytesRefAggregator.SingleState.class.getName(), spec.stateWrapperClassName());

        // Intermediate state
        assertEquals(2, spec.intermediateState().size());
        assertEquals("max", spec.intermediateState().get(0).name());
        assertEquals("BYTES_REF", spec.intermediateState().get(0).elementType());
        assertEquals("seen", spec.intermediateState().get(1).name());
        assertEquals("BOOLEAN", spec.intermediateState().get(1).elementType());
    }

    public void testCustomStateAggregatorClassGeneration() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        assertNotNull(aggregatorClass);
        assertTrue(AggregatorFunction.class.isAssignableFrom(aggregatorClass));

        // Verify the class has the expected methods
        assertHasMethod(aggregatorClass, "create", true);
        assertHasMethod(aggregatorClass, "intermediateStateDesc", true);
        assertHasMethod(aggregatorClass, "addRawInput", false);
        assertHasMethod(aggregatorClass, "addIntermediateInput", false);
        assertHasMethod(aggregatorClass, "evaluateIntermediate", false);
        assertHasMethod(aggregatorClass, "evaluateFinal", false);
        assertHasMethod(aggregatorClass, "close", false);
    }

    public void testCustomStateAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Create a page with some BytesRef values: ["apple", "zebra", "banana"]
        // Max should be "zebra"
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3);
        builder.appendBytesRef(new BytesRef("apple"));
        builder.appendBytesRef(new BytesRef("zebra"));
        builder.appendBytesRef(new BytesRef("banana"));
        BytesRefBlock bytesRefBlock = builder.build();
        Page page = new Page(bytesRefBlock);

        // Create a mask that includes all positions
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, 3);

        // Add raw input
        aggregator.addRawInput(page, mask);

        // Evaluate final result
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a BytesRefBlock with value "zebra"
        assertTrue("Result should be a BytesRefBlock", results[0] instanceof BytesRefBlock);
        BytesRefBlock resultBlock = (BytesRefBlock) results[0];
        assertEquals("Result should have 1 position", 1, resultBlock.getPositionCount());
        BytesRef scratch = new BytesRef();
        assertEquals("Max should be 'zebra'", new BytesRef("zebra"), resultBlock.getBytesRef(0, scratch));

        // Clean up
        results[0].close();
        mask.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testCustomStateAggregatorEmptyInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends AggregatorFunction> aggregatorClass = generator.getOrGenerateAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", DriverContext.class, List.class);
        List<Integer> channels = List.of(0);
        AggregatorFunction aggregator = (AggregatorFunction) createMethod.invoke(null, driverContext, channels);

        // Don't add any input - evaluate immediately
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is null (no values seen)
        assertEquals("Result should have 1 position", 1, results[0].getPositionCount());
        assertTrue("Result should be null", results[0].isNull(0));

        // Clean up
        results[0].close();
        aggregator.close();
    }

    // ========================================================================================
    // Grouping aggregator with custom state tests
    // ========================================================================================

    /**
     * Test aggregator class with custom state implementing both AggregatorState and GroupingAggregatorState.
     * This is the pattern used by MaxBytesRefAggregator for grouping.
     */
    @RuntimeAggregator(
        intermediateState = {
            @RuntimeIntermediateState(name = "max", type = "BYTES_REF"),
            @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") },
        grouping = true
    )
    public static class TestMaxBytesRefGroupingAggregator {
        private static boolean isBetter(BytesRef value, BytesRef otherValue) {
            return value.compareTo(otherValue) > 0;
        }

        // Single state methods
        public static SingleState initSingle(DriverContext driverContext) {
            return new SingleState(driverContext.breaker());
        }

        public static void combine(SingleState state, BytesRef value) {
            state.add(value);
        }

        public static void combineIntermediate(SingleState state, BytesRef value, boolean seen) {
            if (seen) {
                combine(state, value);
            }
        }

        public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
            return state.toBlock(driverContext);
        }

        // Grouping state methods
        public static GroupingState initGrouping(DriverContext driverContext) {
            return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
        }

        public static void combine(GroupingState state, int groupId, BytesRef value) {
            state.add(groupId, value);
        }

        public static void combineIntermediate(GroupingState state, int groupId, BytesRef value, boolean seen) {
            if (seen) {
                state.add(groupId, value);
            }
        }

        public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
            return state.toBlock(selected, ctx.driverContext());
        }

        public static class SingleState implements AggregatorState {
            private final BreakingBytesRefBuilder internalState;
            private boolean seen;

            public SingleState(CircuitBreaker breaker) {
                this.internalState = new BreakingBytesRefBuilder(breaker, "test_max_bytes_ref_aggregator");
                this.seen = false;
            }

            public void add(BytesRef value) {
                if (seen == false || isBetter(value, internalState.bytesRefView())) {
                    seen = true;
                    internalState.grow(value.length);
                    internalState.setLength(value.length);
                    System.arraycopy(value.bytes, value.offset, internalState.bytes(), 0, value.length);
                }
            }

            @Override
            public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
                blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
                blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
            }

            public Block toBlock(DriverContext driverContext) {
                if (seen == false) {
                    return driverContext.blockFactory().newConstantNullBlock(1);
                }
                return driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
            }

            @Override
            public void close() {
                Releasables.close(internalState);
            }
        }

        public static class GroupingState implements GroupingAggregatorState {
            private final java.util.Map<Integer, BytesRef> values = new java.util.HashMap<>();
            private final java.util.Set<Integer> seenGroups = new java.util.HashSet<>();

            private GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
                // Simple implementation for testing - doesn't use BigArrays
            }

            public void add(int groupId, BytesRef value) {
                BytesRef current = values.get(groupId);
                if (current == null || isBetter(value, current)) {
                    values.put(groupId, BytesRef.deepCopyOf(value));
                }
                seenGroups.add(groupId);
            }

            @Override
            public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
                int count = selected.getPositionCount();
                try (
                    BytesRefBlock.Builder maxBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(count);
                    org.elasticsearch.compute.data.BooleanBlock.Builder seenBuilder = driverContext.blockFactory()
                        .newBooleanBlockBuilder(count)
                ) {
                    for (int i = 0; i < count; i++) {
                        int groupId = selected.getInt(i);
                        BytesRef val = values.get(groupId);
                        if (val != null) {
                            maxBuilder.appendBytesRef(val);
                            seenBuilder.appendBoolean(true);
                        } else {
                            maxBuilder.appendNull();
                            seenBuilder.appendBoolean(false);
                        }
                    }
                    blocks[offset] = maxBuilder.build();
                    blocks[offset + 1] = seenBuilder.build();
                }
            }

            Block toBlock(IntVector selected, DriverContext driverContext) {
                int count = selected.getPositionCount();
                try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(count)) {
                    for (int i = 0; i < count; i++) {
                        int groupId = selected.getInt(i);
                        BytesRef val = values.get(groupId);
                        if (val != null) {
                            builder.appendBytesRef(val);
                        } else {
                            builder.appendNull();
                        }
                    }
                    return builder.build();
                }
            }

            @Override
            public void enableGroupIdTracking(SeenGroupIds seen) {
                // No-op for simple implementation
            }

            @Override
            public void close() {
                values.clear();
                seenGroups.clear();
            }
        }
    }

    public void testCustomGroupingStateAggregatorSpecCreation() {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefGroupingAggregator.class);

        assertEquals(TestMaxBytesRefGroupingAggregator.class, spec.declaringClass());
        assertTrue("Should have custom grouping state", spec.hasCustomGroupingState());
        assertNotNull("Should have initGrouping method", spec.initGroupingMethod());
        assertEquals("initGrouping", spec.initGroupingMethod().getName());
        assertNotNull("Should have grouping combine method", spec.groupingCombineMethod());
        assertEquals("combine", spec.groupingCombineMethod().getName());
        assertEquals(TestMaxBytesRefGroupingAggregator.GroupingState.class, spec.groupingStateType());
        assertNotNull("Should have grouping combineIntermediate method", spec.groupingCombineIntermediateMethod());
        assertNotNull("Should have grouping evaluateFinal method", spec.groupingEvaluateFinalMethod());

        // Array state wrapper should be the grouping state class
        assertEquals(TestMaxBytesRefGroupingAggregator.GroupingState.class.getName(), spec.arrayStateWrapperClassName());
    }

    public void testCustomGroupingStateAggregatorClassGeneration() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefGroupingAggregator.class);
        RuntimeAggregatorGenerator generator = new RuntimeAggregatorGenerator(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> groupingClass = generator.getOrGenerateGroupingAggregator(spec);

        assertNotNull(groupingClass);
        assertTrue(GroupingAggregatorFunction.class.isAssignableFrom(groupingClass));

        // Verify the class has the expected methods
        assertHasMethod(groupingClass, "create", true);
        assertHasMethod(groupingClass, "intermediateStateDesc", true);
        assertHasMethod(groupingClass, "intermediateBlockCount", false);
        assertHasMethod(groupingClass, "prepareProcessRawInputPage", false);
        assertHasMethod(groupingClass, "selectedMayContainUnseenGroups", false);
        assertHasMethod(groupingClass, "evaluateIntermediate", false);
        assertHasMethod(groupingClass, "evaluateFinal", false);
        assertHasMethod(groupingClass, "toString", false);
        assertHasMethod(groupingClass, "close", false);
    }

    public void testCustomGroupingStateAggregatorAddRawInput() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefGroupingAggregator.class);
        RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> aggregatorClass = generator.getOrGenerateGroupingAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", List.class, DriverContext.class);
        List<Integer> channels = List.of(0);
        GroupingAggregatorFunction aggregator = (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);

        // Create a page with some BytesRef values: ["apple", "zebra", "banana", "cherry", "mango"]
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(5);
        builder.appendBytesRef(new BytesRef("apple"));
        builder.appendBytesRef(new BytesRef("zebra"));
        builder.appendBytesRef(new BytesRef("banana"));
        builder.appendBytesRef(new BytesRef("cherry"));
        builder.appendBytesRef(new BytesRef("mango"));
        BytesRefBlock valueBlock = builder.build();
        Page page = new Page(valueBlock);

        // Create group IDs: [0, 0, 1, 1, 0]
        // Group 0: "apple", "zebra", "mango" -> max = "zebra"
        // Group 1: "banana", "cherry" -> max = "cherry"
        IntVector.Builder groupBuilder = blockFactory.newIntVectorBuilder(5);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(0);
        IntVector groupIds = groupBuilder.build();

        // Prepare and add raw input
        SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
        GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessRawInputPage(seenGroupIds, page);
        assertNotNull("AddInput should not be null", addInput);

        // Add input for all positions
        addInput.add(0, groupIds);
        addInput.close();

        // Evaluate final result for both groups
        IntVector.Builder selectedBuilder = blockFactory.newIntVectorBuilder(2);
        selectedBuilder.appendInt(0);
        selectedBuilder.appendInt(1);
        IntVector selected = selectedBuilder.build();
        Block[] results = new Block[1];
        GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
        aggregator.evaluateFinal(results, 0, selected, ctx);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result is a BytesRefBlock with values ["zebra", "cherry"]
        assertTrue("Result should be a BytesRefBlock", results[0] instanceof BytesRefBlock);
        BytesRefBlock resultBlock = (BytesRefBlock) results[0];
        assertEquals("Result should have 2 positions", 2, resultBlock.getPositionCount());
        BytesRef scratch = new BytesRef();
        assertEquals("Group 0 max should be 'zebra'", new BytesRef("zebra"), resultBlock.getBytesRef(0, scratch));
        assertEquals("Group 1 max should be 'cherry'", new BytesRef("cherry"), resultBlock.getBytesRef(1, scratch));

        // Clean up
        results[0].close();
        selected.close();
        groupIds.close();
        page.releaseBlocks();
        aggregator.close();
    }

    public void testCustomGroupingStateAggregatorMultipleGroups() throws Exception {
        AggregatorSpec spec = AggregatorSpec.from(TestMaxBytesRefGroupingAggregator.class);
        RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(getClass().getClassLoader());

        Class<? extends GroupingAggregatorFunction> aggregatorClass = generator.getOrGenerateGroupingAggregator(spec);

        // Create a DriverContext for testing
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test"), bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        // Call the static create method
        Method createMethod = aggregatorClass.getMethod("create", List.class, DriverContext.class);
        List<Integer> channels = List.of(0);
        GroupingAggregatorFunction aggregator = (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);

        // Create a page with some BytesRef values
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(6);
        builder.appendBytesRef(new BytesRef("a"));
        builder.appendBytesRef(new BytesRef("b"));
        builder.appendBytesRef(new BytesRef("c"));
        builder.appendBytesRef(new BytesRef("x"));
        builder.appendBytesRef(new BytesRef("y"));
        builder.appendBytesRef(new BytesRef("z"));
        BytesRefBlock valueBlock = builder.build();
        Page page = new Page(valueBlock);

        // Create group IDs: [0, 1, 2, 0, 1, 2]
        // Group 0: "a", "x" -> max = "x"
        // Group 1: "b", "y" -> max = "y"
        // Group 2: "c", "z" -> max = "z"
        IntVector.Builder groupBuilder = blockFactory.newIntVectorBuilder(6);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(2);
        groupBuilder.appendInt(0);
        groupBuilder.appendInt(1);
        groupBuilder.appendInt(2);
        IntVector groupIds = groupBuilder.build();

        // Prepare and add raw input
        SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
        GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessRawInputPage(seenGroupIds, page);
        addInput.add(0, groupIds);
        addInput.close();

        // Evaluate final result for all groups
        IntVector.Builder selectedBuilder = blockFactory.newIntVectorBuilder(3);
        selectedBuilder.appendInt(0);
        selectedBuilder.appendInt(1);
        selectedBuilder.appendInt(2);
        IntVector selected = selectedBuilder.build();
        Block[] results = new Block[1];
        GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
        aggregator.evaluateFinal(results, 0, selected, ctx);

        assertNotNull("Result block should not be null", results[0]);

        // Verify the result
        assertTrue("Result should be a BytesRefBlock", results[0] instanceof BytesRefBlock);
        BytesRefBlock resultBlock = (BytesRefBlock) results[0];
        assertEquals("Result should have 3 positions", 3, resultBlock.getPositionCount());
        BytesRef scratch = new BytesRef();
        assertEquals("Group 0 max should be 'x'", new BytesRef("x"), resultBlock.getBytesRef(0, scratch));
        assertEquals("Group 1 max should be 'y'", new BytesRef("y"), resultBlock.getBytesRef(1, scratch));
        assertEquals("Group 2 max should be 'z'", new BytesRef("z"), resultBlock.getBytesRef(2, scratch));

        // Clean up
        results[0].close();
        selected.close();
        groupIds.close();
        page.releaseBlocks();
        aggregator.close();
    }

    private void assertHasMethod(Class<?> clazz, String methodName, boolean expectStatic) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(methodName)) {
                if (expectStatic) {
                    assertTrue("Method " + methodName + " should be static", Modifier.isStatic(method.getModifiers()));
                }
                return;
            }
        }
        fail("Class " + clazz.getName() + " should have method " + methodName);
    }
}
