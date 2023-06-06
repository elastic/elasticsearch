/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

public class TestPhysicalOperationProviders extends AbstractPhysicalOperationProviders {

    private final Page testData;
    private final List<String> columnNames;

    public TestPhysicalOperationProviders(Page testData, List<String> columnNames) {
        this.testData = testData;
        this.columnNames = columnNames;
    }

    @Override
    public PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.appendChannel(attr.id());
            op = op.with(new TestFieldExtractOperatorFactory(attr.name()), layout.build());
        }
        return op;
    }

    @Override
    public PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        for (int i = 0; i < esQueryExec.output().size(); i++) {
            layout.appendChannel(esQueryExec.output().get(i).id());
        }
        return PhysicalOperation.fromSource(new TestSourceOperatorFactory(), layout.build());
    }

    @Override
    public Operator.OperatorFactory ordinalGroupingOperatorFactory(
        PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        BigArrays bigArrays
    ) {
        int channelIndex = source.layout.numberOfChannels();
        return new TestOrdinalsGroupingAggregationOperatorFactory(
            channelIndex,
            aggregatorFactories,
            groupElementType,
            bigArrays,
            attrSource.name()
        );
    }

    private class TestSourceOperator extends SourceOperator {

        boolean finished = false;

        @Override
        public Page getOutput() {
            if (finished == false) {
                finish();
            }

            Block[] fakeSourceAttributesBlocks = new Block[1];
            // a block that contains the position of each document as int
            // will be used to "filter" and extract the block's values later on. Basically, a replacement for _doc, _shard and _segment ids
            IntBlock.Builder docIndexBlockBuilder = IntBlock.newBlockBuilder(testData.getPositionCount());
            for (int i = 0; i < testData.getPositionCount(); i++) {
                docIndexBlockBuilder.appendInt(i);
            }
            fakeSourceAttributesBlocks[0] = docIndexBlockBuilder.build(); // instead of _doc
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
        public SourceOperator get(DriverContext driverContext) {
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
            Block block = extractBlockForColumn(page, columnName);
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
        public Operator get(DriverContext driverContext) {
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
            List<GroupingAggregator.Factory> aggregators,
            Supplier<BlockHash> blockHash,
            String columnName,
            DriverContext driverContext
        ) {
            super(aggregators, blockHash, driverContext);
            this.columnName = columnName;
        }

        @Override
        protected Page wrapPage(Page page) {
            return page.appendBlock(extractBlockForColumn(page, columnName));
        }
    }

    /**
     * Pretends to be the {@link OrdinalsGroupingOperator} but always delegates to the
     * {@link HashAggregationOperator}.
     */
    private class TestOrdinalsGroupingAggregationOperatorFactory implements Operator.OperatorFactory {
        private int groupByChannel;
        private List<GroupingAggregator.Factory> aggregators;
        private ElementType groupElementType;
        private BigArrays bigArrays;
        private String columnName;

        TestOrdinalsGroupingAggregationOperatorFactory(
            int channelIndex,
            List<GroupingAggregator.Factory> aggregatorFactories,
            ElementType groupElementType,
            BigArrays bigArrays,
            String name
        ) {
            this.groupByChannel = channelIndex;
            this.aggregators = aggregatorFactories;
            this.groupElementType = groupElementType;
            this.bigArrays = bigArrays;
            this.columnName = name;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TestHashAggregationOperator(
                aggregators,
                () -> BlockHash.build(List.of(new HashAggregationOperator.GroupSpec(groupByChannel, groupElementType)), bigArrays),
                columnName,
                driverContext
            );
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

    private Block extractBlockForColumn(Page page, String columnName) {
        var columnIndex = -1;
        // locate the block index corresponding to "columnName"
        for (int i = 0, size = columnNames.size(); i < size && columnIndex < 0; i++) {
            if (columnNames.get(i).equals(columnName)) {
                columnIndex = i;
            }
        }
        if (columnIndex < 0) {
            throw new EsqlIllegalArgumentException("Cannot find column named [{}] in {}", columnName, columnNames);
        }
        // this is the first block added by TestSourceOperator
        IntBlock docIndexBlock = page.getBlock(0);
        // use its filtered position to extract the data needed for "columnName" block
        Block loadedBlock = testData.getBlock(columnIndex);
        int[] filteredPositions = new int[docIndexBlock.getPositionCount()];
        for (int c = 0; c < docIndexBlock.getPositionCount(); c++) {
            filteredPositions[c] = (Integer) docIndexBlock.getInt(c);
        }
        return loadedBlock.filter(filteredPositions);
    }
}
