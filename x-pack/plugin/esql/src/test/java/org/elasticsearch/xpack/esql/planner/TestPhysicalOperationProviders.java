/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static java.util.stream.Collectors.joining;
import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.encodeLongitude;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;

public class TestPhysicalOperationProviders extends AbstractPhysicalOperationProviders {
    private final List<IndexPage> indexPages;

    private TestPhysicalOperationProviders(FoldContext foldContext, List<IndexPage> indexPages, AnalysisRegistry analysisRegistry) {
        super(foldContext, analysisRegistry);
        this.indexPages = indexPages;
    }

    public static TestPhysicalOperationProviders create(FoldContext foldContext, List<IndexPage> indexPages) throws IOException {
        return new TestPhysicalOperationProviders(foldContext, indexPages, createAnalysisRegistry());
    }

    public record IndexPage(String index, Page page, List<String> columnNames) {
        OptionalInt columnIndex(String columnName) {
            return IntStream.range(0, columnNames.size()).filter(i -> columnNames.get(i).equals(columnName)).findFirst();
        }
    }

    private static AnalysisRegistry createAnalysisRegistry() throws IOException {
        return new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @Override
    public PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.append(attr);
            op = op.with(new TestFieldExtractOperatorFactory(attr, fieldExtractExec.fieldExtractPreference(attr)), layout.build());
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
            attrSource
        );
    }

    private class TestSourceOperator extends SourceOperator {
        private int index = 0;
        private final DriverContext driverContext;

        TestSourceOperator(DriverContext driverContext) {
            this.driverContext = driverContext;
        }

        @Override
        public Page getOutput() {
            var pageIndex = indexPages.get(index);
            var page = pageIndex.page;
            BlockFactory blockFactory = driverContext.blockFactory();
            DocVector docVector = new DocVector(
                // The shard ID is used to encode the index ID.
                blockFactory.newConstantIntVector(index, page.getPositionCount()),
                blockFactory.newConstantIntVector(0, page.getPositionCount()),
                blockFactory.newIntArrayVector(IntStream.range(0, page.getPositionCount()).toArray(), page.getPositionCount()),
                true
            );
            var block = docVector.asBlock();
            index++;
            return new Page(block);
        }

        @Override
        public boolean isFinished() {
            return index == indexPages.size();
        }

        @Override
        public void finish() {
            index = indexPages.size();
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
        private final Attribute attribute;
        private Page lastPage;
        boolean finished;
        private final FieldExtractPreference extractPreference;

        TestFieldExtractOperator(Attribute attr, FieldExtractPreference extractPreference) {
            this.attribute = attr;
            this.extractPreference = extractPreference;
        }

        @Override
        public void addInput(Page page) {
            lastPage = page.appendBlock(getBlock(page.getBlock(0), attribute, extractPreference));
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
        private final Operator op;
        private final Attribute attribute;

        TestFieldExtractOperatorFactory(Attribute attr, FieldExtractPreference extractPreference) {
            this.op = new TestFieldExtractOperator(attr, extractPreference);
            this.attribute = attr;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return op;
        }

        @Override
        public String describe() {
            return "TestFieldExtractOperator(" + attribute.name() + ")";
        }
    }

    private Block getBlock(DocBlock docBlock, Attribute attribute, FieldExtractPreference extractPreference) {
        if (attribute instanceof UnsupportedAttribute) {
            return docBlock.blockFactory().newConstantNullBlock(docBlock.getPositionCount());
        }
        return extractBlockForColumn(
            docBlock,
            attribute.dataType(),
            extractPreference,
            attribute instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField multiTypeEsField
                ? (indexDoc, blockCopier) -> getBlockForMultiType(indexDoc, multiTypeEsField, blockCopier)
                : (indexDoc, blockCopier) -> extractBlockForSingleDoc(indexDoc, attribute.name(), blockCopier)
        );
    }

    private Block getBlockForMultiType(DocBlock indexDoc, MultiTypeEsField multiTypeEsField, TestBlockCopier blockCopier) {
        var indexId = indexDoc.asVector().shards().getInt(0);
        var indexPage = indexPages.get(indexId);
        var conversion = (AbstractConvertFunction) multiTypeEsField.getConversionExpressionForIndex(indexPage.index);
        Supplier<Block> nulls = () -> indexDoc.blockFactory().newConstantNullBlock(indexDoc.getPositionCount());
        if (conversion == null) {
            return nulls.get();
        }
        var field = (FieldAttribute) conversion.field();
        return indexPage.columnIndex(field.fieldName().string()).isEmpty()
            ? nulls.get()
            : TypeConverter.fromConvertFunction(conversion).convert(extractBlockForSingleDoc(indexDoc, field.fieldName(), blockCopier));
    }

    private Block extractBlockForSingleDoc(DocBlock docBlock, String columnName, TestBlockCopier blockCopier) {
        var indexId = docBlock.asVector().shards().getInt(0);
        var indexPage = indexPages.get(indexId);
        if (MetadataAttribute.INDEX.equals(columnName)) {
            return docBlock.blockFactory()
                .newConstantBytesRefBlockWith(new BytesRef(indexPage.index), blockCopier.docIndices.getPositionCount());
        }
        int columnIndex = indexPage.columnIndex(columnName)
            .orElseThrow(() -> new EsqlIllegalArgumentException("Cannot find column named [{}] in {}", columnName, indexPage.columnNames));
        var originalData = indexPage.page.getBlock(columnIndex);
        return blockCopier.copyBlock(originalData);
    }

    private static void foreachIndexDoc(DocBlock docBlock, Consumer<DocBlock> indexDocConsumer) {
        var currentIndex = -1;
        List<Integer> currentList = null;
        DocVector vector = docBlock.asVector();
        for (int i = 0; i < docBlock.getPositionCount(); i++) {
            int indexId = vector.shards().getInt(i);
            if (indexId != currentIndex) {
                consumeIndexDoc(indexDocConsumer, vector, currentList);
                currentList = new ArrayList<>();
                currentIndex = indexId;
            }
            currentList.add(i);
        }
        consumeIndexDoc(indexDocConsumer, vector, currentList);
    }

    private static void consumeIndexDoc(Consumer<DocBlock> indexDocConsumer, DocVector vector, @Nullable List<Integer> currentList) {
        if (currentList != null) {
            try (DocVector indexDocVector = vector.filter(currentList.stream().mapToInt(Integer::intValue).toArray())) {
                indexDocConsumer.accept(indexDocVector.asBlock());
            }
        }
    }

    private class TestHashAggregationOperator extends HashAggregationOperator {

        private final Attribute attribute;

        TestHashAggregationOperator(
            List<GroupingAggregator.Factory> aggregators,
            Supplier<BlockHash> blockHash,
            Attribute attribute,
            DriverContext driverContext
        ) {
            super(aggregators, blockHash, driverContext);
            this.attribute = attribute;
        }

        @Override
        protected Page wrapPage(Page page) {
            return page.appendBlock(getBlock(page.getBlock(0), attribute, FieldExtractPreference.NONE));
        }
    }

    /**
     * Pretends to be the {@link OrdinalsGroupingOperator} but always delegates to the
     * {@link HashAggregationOperator}.
     */
    private class TestOrdinalsGroupingAggregationOperatorFactory implements Operator.OperatorFactory {
        private final int groupByChannel;
        private final List<GroupingAggregator.Factory> aggregators;
        private final ElementType groupElementType;
        private final BigArrays bigArrays;
        private final Attribute attribute;

        TestOrdinalsGroupingAggregationOperatorFactory(
            int channelIndex,
            List<GroupingAggregator.Factory> aggregatorFactories,
            ElementType groupElementType,
            BigArrays bigArrays,
            Attribute attribute
        ) {
            this.groupByChannel = channelIndex;
            this.aggregators = aggregatorFactories;
            this.groupElementType = groupElementType;
            this.bigArrays = bigArrays;
            this.attribute = attribute;
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
                attribute,
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
        DocBlock docBlock,
        DataType dataType,
        FieldExtractPreference extractPreference,
        BiFunction<DocBlock, TestBlockCopier, Block> extractBlock
    ) {
        try (
            Block.Builder blockBuilder = blockBuilder(
                dataType,
                extractPreference,
                docBlock.getPositionCount(),
                TestBlockFactory.getNonBreakingInstance()
            )
        ) {
            foreachIndexDoc(docBlock, indexDoc -> {
                TestBlockCopier blockCopier = blockCopier(dataType, extractPreference, indexDoc.asVector().docs());
                try (Block blockForIndex = extractBlock.apply(indexDoc, blockCopier)) {
                    blockBuilder.copyFrom(blockForIndex, 0, blockForIndex.getPositionCount());
                }
            });
            var result = blockBuilder.build();
            assert result.getPositionCount() == docBlock.getPositionCount()
                : "Expected " + docBlock.getPositionCount() + " rows, got " + result.getPositionCount();
            return result;
        }
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
                    if (count == 0) {
                        builder.appendNull();
                    } else {
                        if (count > 1) {
                            builder.beginPositionEntry();
                        }
                        int firstValueIndex = bytesRefBlock.getFirstValueIndex(doc);
                        for (int i = firstValueIndex; i < firstValueIndex + count; i++) {
                            builder.appendLong(encode(bytesRefBlock.getBytesRef(i, scratch)));
                        }
                        if (count > 1) {
                            builder.endPositionEntry();
                        }
                    }
                }
                return builder.build();
            }
        }

        private static TestSpatialPointStatsBlockCopier create(IntVector docIndices, DataType dataType) {
            Function<BytesRef, Long> encoder = switch (dataType) {
                case GEO_POINT -> SpatialCoordinateTypes.GEO::wkbAsLong;
                case CARTESIAN_POINT -> SpatialCoordinateTypes.CARTESIAN::wkbAsLong;
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

    /**
     * geo_shape and cartesian_shape are normally loaded as WKT from source, but for ST_EXTENT_AGG we can load them from doc-values
     * extracting the spatial Extent information. This class is used to convert the test loaded WKB into the int[6] used in the aggregators.
     */
    private abstract static class TestSpatialShapeExtentBlockCopier extends TestBlockCopier {
        protected final SpatialEnvelopeVisitor.PointVisitor pointVisitor;
        private final SpatialEnvelopeVisitor visitor;

        private TestSpatialShapeExtentBlockCopier(IntVector docIndices, SpatialEnvelopeVisitor.PointVisitor pointVisitor) {
            super(docIndices);
            this.pointVisitor = pointVisitor;
            this.visitor = new SpatialEnvelopeVisitor(pointVisitor);
        }

        @Override
        protected Block copyBlock(Block originalData) {
            BytesRef scratch = new BytesRef(100);
            BytesRefBlock bytesRefBlock = (BytesRefBlock) originalData;
            try (IntBlock.Builder builder = bytesRefBlock.blockFactory().newIntBlockBuilder(docIndices.getPositionCount())) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    int count = bytesRefBlock.getValueCount(doc);
                    if (count == 0) {
                        builder.appendNull();
                    } else {
                        pointVisitor.reset();
                        int firstValueIndex = bytesRefBlock.getFirstValueIndex(doc);
                        for (int i = firstValueIndex; i < firstValueIndex + count; i++) {
                            BytesRef wkb = bytesRefBlock.getBytesRef(i, scratch);
                            Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                            geometry.visit(visitor);
                        }
                        encodeExtent(builder);
                    }
                }
                return builder.build();
            }
        }

        protected abstract void encodeExtent(IntBlock.Builder builder);

        private static TestSpatialShapeExtentBlockCopier create(IntVector docIndices, DataType dataType) {
            return switch (dataType) {
                case GEO_SHAPE -> new TestGeoCopier(docIndices);
                case CARTESIAN_SHAPE -> new TestCartesianCopier(docIndices);
                default -> throw new IllegalArgumentException("Unsupported spatial data type: " + dataType);
            };
        }

        private static class TestGeoCopier extends TestSpatialShapeExtentBlockCopier {
            private TestGeoCopier(IntVector docIndices) {
                super(docIndices, new SpatialEnvelopeVisitor.GeoPointVisitor(SpatialEnvelopeVisitor.WrapLongitude.WRAP));
            }

            @Override
            protected void encodeExtent(IntBlock.Builder builder) {
                // We store the 6 values as a single multi-valued field, in the same order as the fields in the Extent class
                // This requires that consumers also know the meaning of the values, which they can learn from the Extent class
                SpatialEnvelopeVisitor.GeoPointVisitor visitor = (SpatialEnvelopeVisitor.GeoPointVisitor) pointVisitor;
                builder.beginPositionEntry();
                builder.appendInt(CoordinateEncoder.GEO.encodeY(visitor.getTop()));
                builder.appendInt(CoordinateEncoder.GEO.encodeY(visitor.getBottom()));
                builder.appendInt(encodeLongitude(visitor.getNegLeft()));
                builder.appendInt(encodeLongitude(visitor.getNegRight()));
                builder.appendInt(encodeLongitude(visitor.getPosLeft()));
                builder.appendInt(encodeLongitude(visitor.getPosRight()));
                builder.endPositionEntry();
            }
        }

        private static class TestCartesianCopier extends TestSpatialShapeExtentBlockCopier {
            private TestCartesianCopier(IntVector docIndices) {
                super(docIndices, new SpatialEnvelopeVisitor.CartesianPointVisitor());
            }

            @Override
            protected void encodeExtent(IntBlock.Builder builder) {
                // We store the 4 values as a single multi-valued field, in the same order as the fields in the Rectangle class
                // This requires that consumers also know the meaning of the values, which they can learn from the Rectangle class
                SpatialEnvelopeVisitor.CartesianPointVisitor visitor = (SpatialEnvelopeVisitor.CartesianPointVisitor) pointVisitor;
                builder.beginPositionEntry();
                builder.appendInt(CoordinateEncoder.CARTESIAN.encodeX(visitor.getMinX()));
                builder.appendInt(CoordinateEncoder.CARTESIAN.encodeX(visitor.getMaxX()));
                builder.appendInt(CoordinateEncoder.CARTESIAN.encodeY(visitor.getMaxY()));
                builder.appendInt(CoordinateEncoder.CARTESIAN.encodeY(visitor.getMinY()));
                builder.endPositionEntry();
            }
        }
    }

    private static Block.Builder blockBuilder(
        DataType dataType,
        FieldExtractPreference extractPreference,
        int estimatedSize,
        BlockFactory blockFactory
    ) {
        ElementType elementType = switch (dataType) {
            case SHORT -> ElementType.INT;
            case FLOAT, HALF_FLOAT, SCALED_FLOAT -> ElementType.DOUBLE;
            default -> PlannerUtils.toElementType(dataType);
        };
        if (extractPreference == DOC_VALUES && DataType.isSpatialPoint(dataType)) {
            return blockFactory.newLongBlockBuilder(estimatedSize);
        } else if (extractPreference == EXTRACT_SPATIAL_BOUNDS && DataType.isSpatial(dataType)) {
            return blockFactory.newIntBlockBuilder(estimatedSize);
        } else {
            return elementType.newBlockBuilder(estimatedSize, blockFactory);
        }
    }

    private static TestBlockCopier blockCopier(DataType dataType, FieldExtractPreference extractPreference, IntVector docIndices) {
        if (extractPreference == DOC_VALUES && DataType.isSpatialPoint(dataType)) {
            return TestSpatialPointStatsBlockCopier.create(docIndices, dataType);
        } else if (extractPreference == EXTRACT_SPATIAL_BOUNDS && DataType.isSpatial(dataType)) {
            return TestSpatialShapeExtentBlockCopier.create(docIndices, dataType);
        } else {
            return new TestBlockCopier(docIndices);
        }
    }
}
