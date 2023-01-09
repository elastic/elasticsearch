/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TestPhysicalOperationProviders extends AbstractPhysicalOperationProviders {

    private final Page testData;
    private final List<String> columnNames;

    public TestPhysicalOperationProviders(Page testData, List<String> columnNames) {
        this.testData = testData;
        this.columnNames = columnNames;
    }

    @Override
    public PhysicalOperation getFieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.appendChannel(attr.id());
            op = op.with(new TestFieldExtractOperatorFactory(attr.name()), layout.build());
        }
        return op;
    }

    @Override
    public PhysicalOperation getSourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        for (int i = 0; i < esQueryExec.output().size(); i++) {
            layout.appendChannel(esQueryExec.output().get(i).id());
        }
        return PhysicalOperation.fromSource(new TestSourceOperatorFactory(), layout.build());
    }

    @Override
    public Operator.OperatorFactory groupingOperatorFactory(
        PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregatorFactories,
        Attribute attrSource,
        Supplier<BlockHash> blockHash
    ) {
        int channelIndex = source.layout.numberOfChannels();
        return new TestHashAggregationOperatorFactory(channelIndex, aggregatorFactories, blockHash, attrSource.name());
    }

    private class TestSourceOperator extends SourceOperator {

        boolean finished = false;

        @Override
        public Page getOutput() {
            if (finished == false) {
                finish();
            }

            Block[] fakeSourceAttributesBlocks = new Block[3];
            // a block that contains the position of each document as int
            // will be used to "filter" and extract the block's values later on. Basically, a replacement for _doc, _shard and _segment ids
            IntBlock.Builder docIndexBlockBuilder = IntBlock.newBlockBuilder(testData.getPositionCount());
            for (int i = 0; i < testData.getPositionCount(); i++) {
                docIndexBlockBuilder.appendInt(i);
            }
            fakeSourceAttributesBlocks[0] = docIndexBlockBuilder.build();
            fakeSourceAttributesBlocks[1] = IntBlock.newConstantBlockWith(0, testData.getPositionCount());
            fakeSourceAttributesBlocks[2] = IntBlock.newConstantBlockWith(0, testData.getPositionCount());
            Page newPageWithSourceAttributes = new Page(fakeSourceAttributesBlocks);
            return newPageWithSourceAttributes;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public void close() {

        }
    }

    private class TestSourceOperatorFactory implements SourceOperatorFactory {

        SourceOperator op = new TestSourceOperator();

        @Override
        public SourceOperator get() {
            return op;
        }

        @Override
        public String describe() {
            return "TestSourceOperator";
        }
    }

    private class TestFieldExtractOperator implements Operator {

        private Page lastPage;
        boolean finished;
        String columnName;

        TestFieldExtractOperator(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public void addInput(Page page) {
            Block block = maybeConvertToLongBlock(extractBlockForColumn(page, columnName));
            lastPage = page.appendBlock(block);
        }

        @Override
        public Page getOutput() {
            Page l = lastPage;
            lastPage = null;
            return l;
        }

        @Override
        public boolean isFinished() {
            return finished && lastPage == null;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return lastPage == null;
        }

        @Override
        public void close() {

        }
    }

    private class TestFieldExtractOperatorFactory implements Operator.OperatorFactory {

        final String columnName;
        final Operator op;

        TestFieldExtractOperatorFactory(String columnName) {
            this.columnName = columnName;
            this.op = new TestFieldExtractOperator(columnName);
        }

        @Override
        public Operator get() {
            return op;
        }

        @Override
        public String describe() {
            return "TestFieldExtractOperator";
        }
    }

    private class TestHashAggregationOperator extends HashAggregationOperator {

        private final String columnName;

        TestHashAggregationOperator(
            int groupByChannel,
            List<GroupingAggregator.GroupingAggregatorFactory> aggregators,
            Supplier<BlockHash> blockHash,
            String columnName
        ) {
            super(groupByChannel, aggregators, blockHash);
            this.columnName = columnName;
        }

        @Override
        public void addInput(Page page) {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");

            Block block = maybeConvertToLongBlock(extractBlockForColumn(page, columnName));
            int positionCount = block.getPositionCount();

            final LongBlock groupIdBlock;
            if (block.asVector() != null) {
                long[] groups = new long[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    long bucketOrd = blockHash().add(block, i);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                    }
                    groups[i] = bucketOrd;
                }
                groupIdBlock = new LongArrayVector(groups, positionCount).asBlock();
            } else {
                final LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
                for (int i = 0; i < positionCount; i++) {
                    if (block.isNull(i)) {
                        builder.appendNull();
                    } else {
                        long bucketOrd = blockHash().add(block, i);
                        if (bucketOrd < 0) { // already seen
                            bucketOrd = -1 - bucketOrd;
                        }
                        builder.appendLong(bucketOrd);
                    }
                }
                groupIdBlock = builder.build();
            }

            for (GroupingAggregator aggregator : aggregators()) {
                aggregator.processPage(groupIdBlock, page);
            }
        }
    }

    private class TestHashAggregationOperatorFactory implements Operator.OperatorFactory {
        private int groupByChannel;
        private List<GroupingAggregator.GroupingAggregatorFactory> aggregators;
        private Supplier<BlockHash> blockHash;
        private String columnName;

        TestHashAggregationOperatorFactory(
            int channelIndex,
            List<GroupingAggregator.GroupingAggregatorFactory> aggregatorFactories,
            Supplier<BlockHash> blockHash,
            String name
        ) {
            this.groupByChannel = channelIndex;
            this.aggregators = aggregatorFactories;
            this.blockHash = blockHash;
            this.columnName = name;
        }

        @Override
        public Operator get() {
            return new TestHashAggregationOperator(groupByChannel, aggregators, blockHash, columnName);
        }

        @Override
        public String describe() {
            return "TestHashAggregationOperator(mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + ")";
        }
    }

    private Block maybeConvertToLongBlock(Block block) {
        int positionCount = block.getPositionCount();
        if (block.elementType() == ElementType.INT) { // the hash is using longs only, so make it a Long block
            LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    builder.appendNull();
                } else {
                    builder.appendLong(((IntBlock) block).getInt(i));
                }
            }
            return builder.build();
        }
        return block;
    }

    private Block extractBlockForColumn(Page page, String columnName) {
        var columnIndex = -1;
        var i = 0;
        // locate the block index corresponding to "columnName"
        while (columnIndex < 0) {
            if (columnNames.get(i).equals(columnName)) {
                columnIndex = i;
            }
            i++;
        }
        // this is the first block added by TestSourceOperator
        Block docIndexBlock = page.getBlock(0);
        // use its filtered position to extract the data needed for "columnName" block
        Block loadedBlock = testData.getBlock(columnIndex);
        int[] filteredPositions = new int[docIndexBlock.getPositionCount()];
        for (int c = 0; c < docIndexBlock.getPositionCount(); c++) {
            filteredPositions[c] = (Integer) docIndexBlock.getObject(c);
        }
        return loadedBlock.filter(filteredPositions);
    }
}
