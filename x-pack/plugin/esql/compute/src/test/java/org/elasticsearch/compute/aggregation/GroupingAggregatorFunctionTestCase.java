/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TestBlockFactory;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.compute.data.BlockTestUtils.append;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class GroupingAggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels);

    protected final int aggregatorIntermediateBlockCount() {
        try (var agg = aggregatorFunction(List.of()).aggregator(driverContext())) {
            return agg.intermediateBlockCount();
        }
    }

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleGroup(List<Page> input, Block result, int position, Long group);

    @Override
    protected final Operator.OperatorFactory simpleWithMode(AggregatorMode mode) {
        List<Integer> channels = mode.isInputPartial() ? range(1, 1 + aggregatorIntermediateBlockCount()).boxed().toList() : List.of(1);
        int emitChunkSize = between(100, 200);

        AggregatorFunctionSupplier supplier = aggregatorFunction(channels);
        if (randomBoolean()) {
            supplier = chunkGroups(emitChunkSize, supplier);
        }
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new HashAggregationOperator.GroupSpec(0, ElementType.LONG)),
            List.of(supplier.groupingAggregatorFactory(mode)),
            randomPageSize()
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "HashAggregationOperator[mode = <not-needed>, aggs = " + expectedDescriptionOfAggregator() + "]";
    }

    @Override
    protected final String expectedToStringOfSimple() {
        String hash = "blockHash=LongBlockHash{channel=0, entries=0, seenNull=false}";
        String type = getClass().getSimpleName().replace("Tests", "");
        return "HashAggregationOperator["
            + hash
            + ", aggregators=[GroupingAggregator[aggregatorFunction="
            + type
            + "[channels=[1]], mode=SINGLE]]]";
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
        SeenGroups seenGroups = seenGroups(input);

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(seenGroups.size()));

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
                        case BYTES_REF -> new BytesRef(randomAlphaOfLength(3));
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

    public void testMulitvaluedNullGroup() {
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
            public AggregatorFunction aggregator(DriverContext driverContext) {
                return supplier.aggregator(driverContext);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
                return new GroupingAggregatorFunction() {
                    GroupingAggregatorFunction delegate = supplier.groupingAggregator(driverContext);
                    BitArray seenGroupIds = new BitArray(0, nonBreakingBigArrays());

                    @Override
                    public AddInput prepareProcessPage(SeenGroupIds ignoredSeenGroupIds, Page page) {
                        return new AddInput() {
                            AddInput delegateAddInput = delegate.prepareProcessPage(bigArrays -> {
                                BitArray seen = new BitArray(0, bigArrays);
                                seen.or(seenGroupIds);
                                return seen;
                            }, page);

                            @Override
                            public void add(int positionOffset, IntBlock groupIds) {
                                for (int offset = 0; offset < groupIds.getPositionCount(); offset += emitChunkSize) {
                                    IntBlock.Builder builder = blockFactory().newIntBlockBuilder(emitChunkSize);
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
                                    delegateAddInput.add(positionOffset + offset, builder.build());
                                }
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
                        };
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
                    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, DriverContext driverContext1) {
                        delegate.evaluateFinal(blocks, offset, selected, driverContext);
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

}
