/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static java.util.stream.Collectors.joining;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;

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
            layout.append(attr);
            op = op.with(
                new TestFieldExtractOperatorFactory(attr, PlannerUtils.extractPreference(fieldExtractExec.hasDocValuesAttribute(attr))),
                layout.build()
            );
        }
        return op;
    }

    @Override
    public PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        return PhysicalOperation.fromSource(new TestSourceOperatorFactory(), layout.build());
    }

    @Override
    public Operator.OperatorFactory ordinalGroupingOperatorFactory(
        PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        LocalExecutionPlannerContext context
    ) {
        int channelIndex = source.layout.numberOfChannels();
        return new TestOrdinalsGroupingAggregationOperatorFactory(
            channelIndex,
            aggregatorFactories,
            groupElementType,
            context.bigArrays(),
            attrSource.name()
        );
    }

    private class TestSourceOperator extends SourceOperator {

        boolean finished = false;
        private final DriverContext driverContext;

        TestSourceOperator(DriverContext driverContext) {
            this.driverContext = driverContext;
        }

        @Override
        public Page getOutput() {
            if (finished == false) {
                finish();
            }

            BlockFactory blockFactory = driverContext.blockFactory();
            DocVector docVector = new DocVector(
                blockFactory.newConstantIntVector(0, testData.getPositionCount()),
                blockFactory.newConstantIntVector(0, testData.getPositionCount()),
                blockFactory.newIntArrayVector(IntStream.range(0, testData.getPositionCount()).toArray(), testData.getPositionCount()),
                true
            );
            return new Page(docVector.asBlock());
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

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new TestSourceOperator(driverContext);
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
        private final DataType dataType;
        private final MappedFieldType.FieldExtractPreference extractPreference;

        TestFieldExtractOperator(String columnName, DataType dataType, MappedFieldType.FieldExtractPreference extractPreference) {
            assert columnNames.contains(columnName);
            this.columnName = columnName;
            this.dataType = dataType;
            this.extractPreference = extractPreference;
        }

        @Override
        public void addInput(Page page) {
            Block block = extractBlockForColumn(page, columnName, dataType, extractPreference);
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
        final Operator op;
        private String columnName;

        TestFieldExtractOperatorFactory(Attribute attr, MappedFieldType.FieldExtractPreference extractPreference) {
            this.op = new TestFieldExtractOperator(attr.name(), attr.dataType(), extractPreference);
            this.columnName = attr.name();
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return op;
        }

        @Override
        public String describe() {
            return "TestFieldExtractOperator(" + columnName + ")";
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
            return page.appendBlock(extractBlockForColumn(page, columnName, null, NONE));
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
            Random random = Randomness.get();
            int pageSize = random.nextBoolean() ? randomIntBetween(random, 1, 16) : randomIntBetween(random, 1, 10 * 1024);
            return new TestHashAggregationOperator(
                aggregators,
                () -> BlockHash.build(
                    List.of(new BlockHash.GroupSpec(groupByChannel, groupElementType)),
                    driverContext.blockFactory(),
                    pageSize,
                    false
                ),
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

    private Block extractBlockForColumn(
        Page page,
        String columnName,
        DataType dataType,
        MappedFieldType.FieldExtractPreference extractPreference
    ) {
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
        DocBlock docBlock = page.getBlock(0);
        IntVector docIndices = docBlock.asVector().docs();
        Block originalData = testData.getBlock(columnIndex);
        var blockCopier = shouldMapToDocValues(dataType, extractPreference)
            ? TestSpatialPointStatsBlockCopier.create(docIndices, dataType)
            : new TestBlockCopier(docIndices);
        return blockCopier.copyBlock(originalData);
    }

    private boolean shouldMapToDocValues(DataType dataType, MappedFieldType.FieldExtractPreference extractPreference) {
        return extractPreference == DOC_VALUES && DataType.isSpatialPoint(dataType);
    }

    private static class TestBlockCopier {

        protected final IntVector docIndices;

        private TestBlockCopier(IntVector docIndices) {
            this.docIndices = docIndices;
        }

        protected Block copyBlock(Block originalData) {
            try (
                Block.Builder builder = originalData.elementType()
                    .newBlockBuilder(docIndices.getPositionCount(), TestBlockFactory.getNonBreakingInstance())
            ) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    builder.copyFrom(originalData, doc, doc + 1);
                }
                return builder.build();
            }
        }
    }

    /**
     * geo_point and cartesian_point are normally loaded as WKT from source, but for aggregations we can load them as doc-values
     * which are encoded Long values. This class is used to convert the test loaded WKB into encoded longs for the aggregators.
     * TODO: We need a different solution to support geo_shape and cartesian_shape
     */
    private abstract static class TestSpatialPointStatsBlockCopier extends TestBlockCopier {

        private TestSpatialPointStatsBlockCopier(IntVector docIndices) {
            super(docIndices);
        }

        protected abstract long encode(BytesRef wkb);

        @Override
        protected Block copyBlock(Block originalData) {
            BytesRef scratch = new BytesRef(100);
            BytesRefBlock bytesRefBlock = (BytesRefBlock) originalData;
            try (LongBlock.Builder builder = bytesRefBlock.blockFactory().newLongBlockBuilder(docIndices.getPositionCount())) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    int count = bytesRefBlock.getValueCount(doc);
                    int i = bytesRefBlock.getFirstValueIndex(doc);
                    if (count == 0) {
                        builder.appendNull();
                    } else {
                        for (int v = 0; v < count; v++) {
                            builder.appendLong(encode(bytesRefBlock.getBytesRef(i, scratch)));
                        }
                    }
                }
                return builder.build();
            }
        }

        private static TestSpatialPointStatsBlockCopier create(IntVector docIndices, DataType dataType) {
            Function<BytesRef, Long> encoder = switch (dataType.esType()) {
                case "geo_point" -> SpatialCoordinateTypes.GEO::wkbAsLong;
                case "cartesian_point" -> SpatialCoordinateTypes.CARTESIAN::wkbAsLong;
                default -> throw new IllegalArgumentException("Unsupported spatial data type: " + dataType);
            };
            return new TestSpatialPointStatsBlockCopier(docIndices) {
                @Override
                protected long encode(BytesRef wkb) {
                    return encoder.apply(wkb);
                }
            };
        }
    }
}
