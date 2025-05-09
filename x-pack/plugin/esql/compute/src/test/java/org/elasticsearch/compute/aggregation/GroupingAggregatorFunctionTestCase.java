/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.ConstantBooleanExpressionEvaluator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.BlockHashWrapper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTypeRandomizer;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AddGarbageRowsSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.compute.test.BlockTestUtils.append;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Shared tests for testing grouped aggregations.
 */
public abstract class GroupingAggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction();

    protected final int aggregatorIntermediateBlockCount() {
        try (var agg = aggregatorFunction().groupingAggregator(driverContext(), List.of())) {
            return agg.intermediateBlockCount();
        }
    }

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleGroup(List<Page> input, Block result, int position, Long group);

    /**
     * Returns the datatype this aggregator accepts. If null, all datatypes are accepted.
     * <p>
     *     Used to generate correct input for aggregators that require specific types.
     *     For example, IP aggregators require BytesRefs with a fixed size.
     * </p>
     */
    @Nullable
    protected DataType acceptedDataType() {
        return null;
    };

    @Override
    protected final Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        return simpleWithMode(options, mode, Function.identity());
    }

    protected List<Integer> channels(AggregatorMode mode) {
        return mode.isInputPartial() ? range(1, 1 + aggregatorIntermediateBlockCount()).boxed().toList() : List.of(1);
    }

    private Operator.OperatorFactory simpleWithMode(
        SimpleOptions options,
        AggregatorMode mode,
        Function<AggregatorFunctionSupplier, AggregatorFunctionSupplier> wrap
    ) {
        int emitChunkSize = between(100, 200);

        AggregatorFunctionSupplier supplier = wrap.apply(aggregatorFunction());
        if (randomBoolean()) {
            supplier = chunkGroups(emitChunkSize, supplier);
        }

        if (options.requiresDeterministicFactory()) {
            return new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(0, ElementType.LONG)),
                mode,
                List.of(supplier.groupingAggregatorFactory(mode, channels(mode))),
                randomPageSize(),
                null
            );
        } else {
            return new RandomizingHashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(0, ElementType.LONG)),
                mode,
                List.of(supplier.groupingAggregatorFactory(mode, channels(mode))),
                randomPageSize(),
                null
            );
        }
    }

    @Override
    protected final Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("HashAggregationOperator[mode = <not-needed>, aggs = " + expectedDescriptionOfAggregator() + "]");
    }

    @Override
    protected final Matcher<String> expectedToStringOfSimple() {
        String hash = "blockHash=LongBlockHash{channel=0, entries=0, seenNull=false}";
        return equalTo(
            "HashAggregationOperator["
                + hash
                + ", aggregators=[GroupingAggregator[aggregatorFunction="
                + expectedToStringOfSimpleAggregator()
                + ", mode=SINGLE]]]"
        );
    }

    protected String expectedToStringOfSimpleAggregator() {
        String type = getClass().getSimpleName().replace("Tests", "");
        return type + "[channels=[1]]";
    }

    private SeenGroups seenGroups(List<Page> input) {
        boolean seenNullGroup = false;
        SortedSet<Long> seenGroups = new TreeSet<>();
        for (Page in : input) {
            Block groups = in.getBlock(0);
            for (int p = 0; p < in.getPositionCount(); p++) {
                if (groups.isNull(p)) {
                    seenNullGroup = true;
                    continue;
                }
                int start = groups.getFirstValueIndex(p);
                int end = start + groups.getValueCount(p);
                for (int g = start; g < end; g++) {
                    seenGroups.add(((LongBlock) groups).getLong(g));
                }
            }
        }
        return new SeenGroups(seenGroups, seenNullGroup);
    }

    private record SeenGroups(SortedSet<Long> nonNull, boolean seenNull) {
        int size() {
            return nonNull.size() + (seenNull ? 1 : 0);
        }
    }

    protected long randomGroupId(int pageSize) {
        int maxGroupId = pageSize < 10 && randomBoolean() ? 4 : 100;
        return randomIntBetween(0, maxGroupId);
    }

    @Override
    protected final void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertSimpleOutput(input, results, true);
    }

    private void assertSimpleOutput(List<Page> input, List<Page> results, boolean assertGroupCount) {
        SeenGroups seenGroups = seenGroups(input);

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        if (assertGroupCount) {
            assertThat(results.get(0).getPositionCount(), equalTo(seenGroups.size()));
        }

        Block groups = results.get(0).getBlock(0);
        Block result = results.get(0).getBlock(1);
        for (int i = 0; i < seenGroups.size(); i++) {
            final Long group;
            if (groups.isNull(i)) {
                group = null;
            } else {
                group = ((LongBlock) groups).getLong(i);
            }
            assertSimpleGroup(input, result, i, group);
        }
    }

    public final void testNullGroupsAndValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(
            new NullInsertingSourceOperator(simpleInput(driverContext.blockFactory(), end), blockFactory)
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testNullGroups() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullGroups(simpleInput(blockFactory, end), blockFactory));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public void testAllKeyNulls() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = new ArrayList<>();
        for (Page p : CannedSourceOperator.collectPages(simpleInput(blockFactory, between(11, 20)))) {
            if (randomBoolean()) {
                input.add(p);
            } else {
                Block[] blocks = new Block[p.getBlockCount()];
                blocks[0] = blockFactory.newConstantNullBlock(p.getPositionCount());
                for (int i = 1; i < blocks.length; i++) {
                    blocks[i] = p.getBlock(i);
                }
                p.getBlock(0).close();
                input.add(new Page(blocks));
            }
        }
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    private SourceOperator nullGroups(SourceOperator source, BlockFactory blockFactory) {
        return new NullInsertingSourceOperator(source, blockFactory) {
            @Override
            protected void appendNull(ElementType elementType, Block.Builder builder, int blockId) {
                if (blockId == 0) {
                    super.appendNull(elementType, builder, blockId);
                } else {
                    // Append a small random value to make sure we don't overflow on things like sums
                    append(builder, switch (elementType) {
                        case BOOLEAN -> randomBoolean();
                        case BYTES_REF -> {
                            if (acceptedDataType() == DataType.IP) {
                                yield new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
                            }
                            yield new BytesRef(randomAlphaOfLength(3));
                        }
                        case FLOAT -> randomFloat();
                        case DOUBLE -> randomDouble();
                        case INT -> 1;
                        case LONG -> 1L;
                        default -> throw new UnsupportedOperationException();
                    });
                }
            }
        };
    }

    public final void testNullValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullValues(simpleInput(driverContext.blockFactory(), end), blockFactory));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testNullValuesInitialIntermediateFinal() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullValues(simpleInput(driverContext.blockFactory(), end), blockFactory));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(
            List.of(
                simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
                simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
                simpleWithMode(AggregatorMode.FINAL).get(driverContext)
            ),
            input.iterator(),
            driverContext
        );
        assertSimpleOutput(origInput, results);
    }

    private SourceOperator nullValues(SourceOperator source, BlockFactory blockFactory) {
        return new NullInsertingSourceOperator(source, blockFactory) {
            @Override
            protected void appendNull(ElementType elementType, Block.Builder builder, int blockId) {
                if (blockId == 0) {
                    ((LongBlock.Builder) builder).appendLong(between(0, 4));
                } else {
                    super.appendNull(elementType, builder, blockId);
                }
            }
        };
    }

    public final void testMultivalued() {
        DriverContext driverContext = driverContext();
        int end = between(1_000, 100_000);
        List<Page> input = CannedSourceOperator.collectPages(
            mergeValues(simpleInput(driverContext.blockFactory(), end), driverContext.blockFactory())
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testMulitvaluedNullGroupsAndValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(
            new NullInsertingSourceOperator(mergeValues(simpleInput(driverContext.blockFactory(), end), blockFactory), blockFactory)
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testMulitvaluedNullGroup() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(1, 2);  // TODO revert
        var inputOperator = nullGroups(mergeValues(simpleInput(driverContext.blockFactory(), end), blockFactory), blockFactory);
        List<Page> input = CannedSourceOperator.collectPages(inputOperator);
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testMulitvaluedNullValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(
            nullValues(mergeValues(simpleInput(blockFactory, end), blockFactory), blockFactory)
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(simple().get(driverContext), input.iterator(), driverContext);
        assertSimpleOutput(origInput, results);
    }

    public final void testNullOnly() {
        DriverContext driverContext = driverContext();
        assertNullOnly(List.of(simple().get(driverContext)), driverContext);
    }

    public final void testNullOnlyInputInitialFinal() {
        DriverContext driverContext = driverContext();
        assertNullOnly(
            List.of(simpleWithMode(AggregatorMode.INITIAL).get(driverContext), simpleWithMode(AggregatorMode.FINAL).get(driverContext)),
            driverContext
        );
    }

    public final void testNullOnlyInputInitialIntermediateFinal() {
        DriverContext driverContext = driverContext();
        assertNullOnly(
            List.of(
                simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
                simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
                simpleWithMode(AggregatorMode.FINAL).get(driverContext)
            ),
            driverContext
        );
    }

    public final void testEmptyInput() {
        DriverContext driverContext = driverContext();
        List<Page> results = drive(simple().get(driverContext), List.<Page>of().iterator(), driverContext);

        assertThat(results, hasSize(0));
    }

    public final void testAllFiltered() {
        Operator.OperatorFactory factory = simpleWithMode(
            SimpleOptions.DEFAULT,
            AggregatorMode.SINGLE,
            agg -> new FilteredAggregatorFunctionSupplier(agg, ConstantBooleanExpressionEvaluator.factory(false))
        );
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 10));
        List<Page> results = drive(factory.get(driverContext), input.iterator(), driverContext);
        assertThat(results, hasSize(1));
        assertOutputFromAllFiltered(results.get(0).getBlock(1));
    }

    public final void testNoneFiltered() {
        Operator.OperatorFactory factory = simpleWithMode(
            SimpleOptions.DEFAULT,
            AggregatorMode.SINGLE,
            agg -> new FilteredAggregatorFunctionSupplier(agg, ConstantBooleanExpressionEvaluator.factory(true))
        );
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 10));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(factory.get(driverContext), input.iterator(), driverContext);
        assertThat(results, hasSize(1));
        assertSimpleOutput(origInput, results);
    }

    public void testSomeFiltered() {
        Operator.OperatorFactory factory = simpleWithMode(
            SimpleOptions.DEFAULT,
            AggregatorMode.SINGLE,
            agg -> new FilteredAggregatorFunctionSupplier(agg, AddGarbageRowsSourceOperator.filterFactory())
        );
        DriverContext driverContext = driverContext();
        // Build the test data
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 10));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        // Sprinkle garbage into it
        input = CannedSourceOperator.collectPages(new AddGarbageRowsSourceOperator(new CannedSourceOperator(input.iterator())));
        List<Page> results = drive(factory.get(driverContext), input.iterator(), driverContext);
        assertThat(results, hasSize(1));

        assertSimpleOutput(origInput, results, false);
    }

    /**
     * Asserts that the output from an empty input is a {@link Block} containing
     * only {@code null}. Override for {@code count} style aggregations that
     * return other sorts of results.
     */
    protected void assertOutputFromAllFiltered(Block b) {
        assertThat(b.areAllValuesNull(), equalTo(true));
        assertThat(b.isNull(0), equalTo(true));
        assertThat(b.getValueCount(0), equalTo(0));
    }

    /**
     * Run the aggregation passing only null values.
     */
    private void assertNullOnly(List<Operator> operators, DriverContext driverContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        try (var groupBuilder = blockFactory.newLongBlockBuilder(1)) {
            if (randomBoolean()) {
                groupBuilder.appendLong(1);
            } else {
                groupBuilder.appendNull();
            }
            List<Page> source = List.of(new Page(groupBuilder.build(), blockFactory.newConstantNullBlock(1)));
            List<Page> results = drive(operators, source.iterator(), driverContext);

            assertThat(results, hasSize(1));
            Block resultBlock = results.get(0).getBlock(1);
            assertOutputFromNullOnly(resultBlock, 0);
        }
    }

    public final void testNullSome() {
        DriverContext driverContext = driverContext();
        assertNullSome(driverContext, List.of(simple().get(driverContext)));
    }

    public final void testNullSomeInitialFinal() {
        DriverContext driverContext = driverContext();
        assertNullSome(
            driverContext,
            List.of(simpleWithMode(AggregatorMode.INITIAL).get(driverContext), simpleWithMode(AggregatorMode.FINAL).get(driverContext))
        );
    }

    public final void testNullSomeInitialIntermediateFinal() {
        DriverContext driverContext = driverContext();
        assertNullSome(
            driverContext,
            List.of(
                simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
                simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
                simpleWithMode(AggregatorMode.FINAL).get(driverContext)
            )
        );
    }

    /**
     * Run the agg on some data where one group is always null.
     */
    private void assertNullSome(DriverContext driverContext, List<Operator> operators) {
        List<Page> inputData = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 1000));
        SeenGroups seenGroups = seenGroups(inputData);

        long nullGroup = randomFrom(seenGroups.nonNull);
        List<Page> source = new ArrayList<>(inputData.size());
        for (Page page : inputData) {
            LongVector groups = page.<LongBlock>getBlock(0).asVector();
            Block values = page.getBlock(1);
            Block.Builder copiedValues = values.elementType().newBlockBuilder(page.getPositionCount(), driverContext.blockFactory());
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (groups.getLong(p) == nullGroup) {
                    copiedValues.appendNull();
                } else {
                    copiedValues.copyFrom(values, p, p + 1);
                }
            }
            Releasables.closeWhileHandlingException(values);
            source.add(new Page(groups.asBlock(), copiedValues.build()));
        }

        List<Page> results = drive(operators, source.iterator(), driverContext);

        assertThat(results, hasSize(1));
        LongVector groups = results.get(0).<LongBlock>getBlock(0).asVector();
        Block resultBlock = results.get(0).getBlock(1);
        boolean foundNullPosition = false;
        for (int p = 0; p < groups.getPositionCount(); p++) {
            if (groups.getLong(p) == nullGroup) {
                foundNullPosition = true;
                assertOutputFromNullOnly(resultBlock, p);
            }
        }
        assertTrue("didn't find the null position. bad position range?", foundNullPosition);
    }

    /**
     * Asserts that the output from a group that contains only null values is
     * a {@link Block} containing only {@code null}. Override for
     * {@code count} style aggregations that return other sorts of results.
     */
    protected void assertOutputFromNullOnly(Block b, int position) {
        assertThat(b.isNull(position), equalTo(true));
        assertThat(b.getValueCount(position), equalTo(0));
    }

    private SourceOperator mergeValues(SourceOperator orig, BlockFactory blockFactory) {
        return new PositionMergingSourceOperator(orig, blockFactory) {
            @Override
            protected Block merge(int blockIndex, Block block) {
                // Merge positions for all blocks but the first. For the first just take the first position.
                if (blockIndex != 0) {
                    return super.merge(blockIndex, block);
                }
                Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount() / 2, blockFactory);
                for (int p = 0; p + 1 < block.getPositionCount(); p += 2) {
                    builder.copyFrom(block, p, p + 1);
                }
                if (block.getPositionCount() % 2 == 1) {
                    builder.copyFrom(block, block.getPositionCount() - 1, block.getPositionCount());
                }
                return builder.build();
            }
        };
    }

    protected static IntStream allValueOffsets(Page page, Long group) {
        Block groupBlock = page.getBlock(0);
        Block valueBlock = page.getBlock(1);
        return IntStream.range(0, page.getPositionCount()).flatMap(p -> {
            if (valueBlock.isNull(p)) {
                return IntStream.of();
            }
            if (group == null) {
                if (false == groupBlock.isNull(p)) {
                    return IntStream.of();
                }
            } else {
                int groupStart = groupBlock.getFirstValueIndex(p);
                int groupEnd = groupStart + groupBlock.getValueCount(p);
                boolean matched = false;
                for (int i = groupStart; i < groupEnd; i++) {
                    if (((LongBlock) groupBlock).getLong(i) == group) {
                        matched = true;
                        break;
                    }
                }
                if (matched == false) {
                    return IntStream.of();
                }
            }
            int start = valueBlock.getFirstValueIndex(p);
            int end = start + valueBlock.getValueCount(p);
            return IntStream.range(start, end);
        });
    }

    protected static Stream<BytesRef> allBytesRefs(Page page, Long group) {
        BytesRefBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getBytesRef(i, new BytesRef()));
    }

    protected static Stream<Boolean> allBooleans(Page page, Long group) {
        BooleanBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getBoolean(i));
    }

    protected static Stream<Float> allFloats(Page page, Long group) {
        FloatBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(b::getFloat);
    }

    protected static DoubleStream allDoubles(Page page, Long group) {
        DoubleBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToDouble(i -> b.getDouble(i));
    }

    protected static IntStream allInts(Page page, Long group) {
        IntBlock b = page.getBlock(1);
        return allValueOffsets(page, group).map(i -> b.getInt(i));
    }

    protected static LongStream allLongs(Page page, Long group) {
        LongBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToLong(i -> b.getLong(i));
    }

    /**
     * Forcibly chunk groups on the way into the aggregator to make sure it can handle chunked
     * groups. This is needed because our chunking logic for groups doesn't bother chunking
     * in non-combinatorial explosion cases. We figure if the could fit into memory then the
     * groups should too. But for testing we'd sometimes like to force chunking just so we
     * run the aggregation with funny chunked inputs.
     */
    private AggregatorFunctionSupplier chunkGroups(int emitChunkSize, AggregatorFunctionSupplier supplier) {
        return new AggregatorFunctionSupplier() {
            @Override
            public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
                return supplier.nonGroupingIntermediateStateDesc();
            }

            @Override
            public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
                return supplier.groupingIntermediateStateDesc();
            }

            @Override
            public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
                return supplier.aggregator(driverContext, channels);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
                return new GroupingAggregatorFunction() {
                    GroupingAggregatorFunction delegate = supplier.groupingAggregator(driverContext, channels);
                    BitArray seenGroupIds = new BitArray(0, nonBreakingBigArrays());

                    @Override
                    public AddInput prepareProcessPage(SeenGroupIds ignoredSeenGroupIds, Page page) {
                        return new AddInput() {
                            final AddInput delegateAddInput = delegate.prepareProcessPage(bigArrays -> {
                                BitArray seen = new BitArray(0, bigArrays);
                                seen.or(seenGroupIds);
                                return seen;
                            }, page);

                            private void addBlock(int positionOffset, IntBlock groupIds) {
                                for (int offset = 0; offset < groupIds.getPositionCount(); offset += emitChunkSize) {
                                    try (IntBlock.Builder builder = blockFactory().newIntBlockBuilder(emitChunkSize)) {
                                        int endP = Math.min(groupIds.getPositionCount(), offset + emitChunkSize);
                                        for (int p = offset; p < endP; p++) {
                                            int start = groupIds.getFirstValueIndex(p);
                                            int count = groupIds.getValueCount(p);
                                            switch (count) {
                                                case 0 -> builder.appendNull();
                                                case 1 -> {
                                                    int group = groupIds.getInt(start);
                                                    seenGroupIds.set(group);
                                                    builder.appendInt(group);
                                                }
                                                default -> {
                                                    int end = start + count;
                                                    builder.beginPositionEntry();
                                                    for (int i = start; i < end; i++) {
                                                        int group = groupIds.getInt(i);
                                                        seenGroupIds.set(group);
                                                        builder.appendInt(group);
                                                    }
                                                    builder.endPositionEntry();
                                                }
                                            }
                                        }
                                        try (IntBlock chunked = builder.build()) {
                                            delegateAddInput.add(positionOffset + offset, chunked);
                                        }
                                    }
                                }
                            }

                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                addBlock(positionOffset, groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                addBlock(positionOffset, groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                int[] chunk = new int[emitChunkSize];
                                for (int offset = 0; offset < groupIds.getPositionCount(); offset += emitChunkSize) {
                                    int count = 0;
                                    for (int i = offset; i < Math.min(groupIds.getPositionCount(), offset + emitChunkSize); i++) {
                                        int group = groupIds.getInt(i);
                                        seenGroupIds.set(group);
                                        chunk[count++] = group;
                                    }
                                    BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance(); // TODO: just for compile
                                    delegateAddInput.add(positionOffset + offset, blockFactory.newIntArrayVector(chunk, count));
                                }
                            }

                            @Override
                            public void close() {
                                delegateAddInput.close();
                            }
                        };
                    }

                    @Override
                    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
                        delegate.selectedMayContainUnseenGroups(seenGroupIds);
                    }

                    @Override
                    public void addIntermediateInput(int positionOffset, IntVector groupIds, Page page) {
                        int[] chunk = new int[emitChunkSize];
                        for (int offset = 0; offset < groupIds.getPositionCount(); offset += emitChunkSize) {
                            int count = 0;
                            for (int i = offset; i < Math.min(groupIds.getPositionCount(), offset + emitChunkSize); i++) {
                                chunk[count++] = groupIds.getInt(i);
                            }
                            BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance(); // TODO: just for compile
                            delegate.addIntermediateInput(positionOffset + offset, blockFactory.newIntArrayVector(chunk, count), page);
                        }
                    }

                    @Override
                    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
                        delegate.addIntermediateRowInput(groupId, input, position);
                    }

                    @Override
                    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
                        delegate.evaluateIntermediate(blocks, offset, selected);
                    }

                    @Override
                    public void evaluateFinal(
                        Block[] blocks,
                        int offset,
                        IntVector selected,
                        GroupingAggregatorEvaluationContext evaluationContext
                    ) {
                        delegate.evaluateFinal(blocks, offset, selected, evaluationContext);
                    }

                    @Override
                    public int intermediateBlockCount() {
                        return delegate.intermediateBlockCount();
                    }

                    @Override
                    public void close() {
                        Releasables.close(delegate, seenGroupIds);
                    }

                    @Override
                    public String toString() {
                        return delegate.toString();
                    }
                };
            }

            @Override
            public String describe() {
                return supplier.describe();
            }
        };
    }

    /**
     * Custom {@link HashAggregationOperator.HashAggregationOperatorFactory} implementation that
     * randomizes the GroupIds block type passed to AddInput.
     * <p>
     *     This helps testing the different overloads of
     *     {@link org.elasticsearch.compute.aggregation.GroupingAggregatorFunction.AddInput#add}
     * </p>
     */
    private record RandomizingHashAggregationOperatorFactory(
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize,
        AnalysisRegistry analysisRegistry
    ) implements Operator.OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            Supplier<BlockHash> blockHashSupplier = () -> {
                BlockHash blockHash = groups.stream().anyMatch(BlockHash.GroupSpec::isCategorize)
                    ? BlockHash.buildCategorizeBlockHash(
                        groups,
                        aggregatorMode,
                        driverContext.blockFactory(),
                        analysisRegistry,
                        maxPageSize
                    )
                    : BlockHash.build(groups, driverContext.blockFactory(), maxPageSize, false);

                return new BlockHashWrapper(driverContext.blockFactory(), blockHash) {
                    @Override
                    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
                        blockHash.add(page, new GroupingAggregatorFunction.AddInput() {
                            @Override
                            public void add(int positionOffset, IntBlock groupIds) {
                                IntBlock newGroupIds = aggregatorMode.isInputPartial()
                                    ? groupIds
                                    : BlockTypeRandomizer.randomizeBlockType(groupIds);
                                addInput.add(positionOffset, newGroupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntArrayBlock groupIds) {
                                add(positionOffset, (IntBlock) groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                                add(positionOffset, (IntBlock) groupIds);
                            }

                            @Override
                            public void add(int positionOffset, IntVector groupIds) {
                                add(positionOffset, groupIds.asBlock());
                            }

                            @Override
                            public void close() {
                                addInput.close();
                            }
                        });
                    }
                };
            };

            return new HashAggregationOperator(aggregators, blockHashSupplier, driverContext);
        }

        @Override
        public String describe() {
            return new HashAggregationOperator.HashAggregationOperatorFactory(
                groups,
                aggregatorMode,
                aggregators,
                maxPageSize,
                analysisRegistry
            ).describe();
        }
    }

}
