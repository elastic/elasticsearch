/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
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
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.compute.aggregation.spatial.SpatialAggregationUtils.encodeLongitude;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS_AND_CENTROID;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_CENTROID;

public class TestPhysicalOperationProviders extends AbstractPhysicalOperationProviders {

    public static final LazyInitializable<Environment, RuntimeException> TEST_ENV = new LazyInitializable<>(
        () -> TestEnvironment.newEnvironment(
            Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
        )
    );

    private final List<IndexPage> indexPages;
    private final UnmappedResolution unmappedResolution;

    private TestPhysicalOperationProviders(
        FoldContext foldContext,
        List<IndexPage> indexPages,
        UnmappedResolution unmappedResolution,
        AnalysisRegistry analysisRegistry
    ) {
        super(foldContext, analysisRegistry);
        this.indexPages = indexPages;
        this.unmappedResolution = unmappedResolution;
    }

    public static TestPhysicalOperationProviders create(
        FoldContext foldContext,
        List<IndexPage> indexPages,
        UnmappedResolution unmappedResolution
    ) throws IOException {
        return new TestPhysicalOperationProviders(foldContext, indexPages, unmappedResolution, createAnalysisRegistry());
    }

    public record IndexPage(String index, Page page, List<String> columnNames, Set<String> mappedFields) {
        Optional<Integer> columnIndex(String columnName) {
            var result = IntStream.range(0, columnNames.size()).filter(i -> columnNames.get(i).equals(columnName)).findFirst();
            return result.isPresent() ? Optional.of(result.getAsInt()) : Optional.empty();
        }
    }

    private static AnalysisRegistry createAnalysisRegistry() throws IOException {
        return new AnalysisModule(
            TEST_ENV.getOrCompute(),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @Override
    public PhysicalOperation fieldExtractPhysicalOperation(
        FieldExtractExec fieldExtractExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
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
    public Operator.OperatorFactory timeSeriesAggregatorOperatorFactory(
        TimeSeriesAggregateExec ts,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        List<BlockHash.GroupSpec> groupSpecs,
        LocalExecutionPlannerContext context,
        int maxPageSize
    ) {
        return new TimeSeriesAggregationOperator.Factory(
            ts.timeBucketRounding(context.foldCtx()),
            ts.timeBucket() != null && ts.timeBucket().dataType() == DataType.DATE_NANOS,
            groupSpecs,
            aggregatorMode,
            aggregatorFactories,
            maxPageSize
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
                ConstantShardContextIndexedByShardId.INSTANCE,
                // The shard ID is used to encode the index ID.
                blockFactory.newConstantIntVector(index, page.getPositionCount()),
                blockFactory.newConstantIntVector(0, page.getPositionCount()),
                blockFactory.newIntArrayVector(IntStream.range(0, page.getPositionCount()).toArray(), page.getPositionCount()),
                DocVector.config().singleSegmentNonDecreasing(true)
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
        private final DriverContext context;
        private final Attribute attribute;
        private Page lastPage;
        boolean finished;
        private final FieldExtractPreference extractPreference;

        TestFieldExtractOperator(DriverContext context, Attribute attr, FieldExtractPreference extractPreference) {
            this.context = context;
            this.attribute = attr;
            this.extractPreference = extractPreference;
        }

        @Override
        public void addInput(Page page) {
            lastPage = page.appendBlock(getBlock(context, page.getBlock(0), attribute, extractPreference));
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
        public boolean canProduceMoreDataWithoutExtraInput() {
            return lastPage != null;
        }

        @Override
        public void close() {

        }
    }

    private class TestFieldExtractOperatorFactory implements Operator.OperatorFactory {
        private final Attribute attribute;
        private final FieldExtractPreference extractPreference;

        private TestFieldExtractOperatorFactory(Attribute attribute, FieldExtractPreference extractPreference) {
            this.attribute = attribute;
            this.extractPreference = extractPreference;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TestFieldExtractOperator(driverContext, attribute, extractPreference);
        }

        @Override
        public String describe() {
            return "TestFieldExtractOperator(" + attribute.name() + ")";
        }
    }

    private Block getBlock(DriverContext context, DocBlock docBlock, Attribute attribute, FieldExtractPreference extractPreference) {
        if (attribute instanceof UnsupportedAttribute) {
            return getNullsBlock(docBlock);
        }
        BiFunction<DocBlock, TestBlockCopier, Block> blockExtraction = getBlockExtraction(context, attribute);
        return extractBlockForColumn(docBlock, attribute.dataType(), extractPreference, blockExtraction);
    }

    private BiFunction<DocBlock, TestBlockCopier, Block> getBlockExtraction(DriverContext context, Attribute attribute) {
        if (attribute instanceof FieldAttribute fa) {
            if (fa.field() instanceof MultiTypeEsField m) {
                return (doc, copier) -> getBlockForMultiType(context, doc, m, copier);

            }
            if (fa.field() instanceof PotentiallyUnmappedKeywordEsField k) {
                return (doc, copier) -> switch (extractBlockForSingleDoc(doc, k.getName(), copier)) {
                    case BlockResultMissing unused -> getNullsBlock(doc);
                    case BlockResultSuccess success -> success.block;
                };
            }
            // For NULL-typed fields (unmapped fields with NULLIFY mode), return nulls
            if (fa.dataType() == DataType.NULL) {
                return (doc, copier) -> getNullsBlock(doc);
            }
        }
        return (indexDoc, blockCopier) -> switch (extractBlockForSingleDoc(indexDoc, attribute.name(), blockCopier)) {
            case BlockResultMissing missing -> {
                if (unmappedResolution == UnmappedResolution.NULLIFY) {
                    yield getNullsBlock(indexDoc);
                }
                throw new EsqlIllegalArgumentException("Cannot find column named [{}] in {}", missing.columnName, missing.columnNames);
            }
            case BlockResultSuccess success -> success.block;
        };
    }

    private Block getBlockForMultiType(
        DriverContext context,
        DocBlock indexDoc,
        MultiTypeEsField multiTypeEsField,
        TestBlockCopier blockCopier
    ) {
        var conversion = getConversion(multiTypeEsField, getIndexPage(indexDoc));
        if (conversion == null) {
            return getNullsBlock(indexDoc);
        }
        return switch (extractBlockForSingleDoc(indexDoc, ((FieldAttribute) conversion.field()).fieldName().string(), blockCopier)) {
            case BlockResultMissing unused -> getNullsBlock(indexDoc);
            case BlockResultSuccess success -> {
                if (success.block.elementType() != PlannerUtils.toElementType(conversion.field().dataType().widenSmallNumeric())
                    && success.block.elementType() == PlannerUtils.toElementType(conversion.dataType().widenSmallNumeric())) {
                    // Block is already in the correct type, we can skip the conversion.
                    yield success.block;
                }
                try (var converter = new TypeConverter(conversion).build(context)) {
                    yield converter.convert(success.block);
                }
            }
        };
    }

    @Nullable
    private static AbstractConvertFunction getConversion(MultiTypeEsField multiTypeEsField, IndexPage indexPage) {
        var conversion = (AbstractConvertFunction) multiTypeEsField.getConversionExpressionForIndex(indexPage.index);
        boolean isPotentiallyUnmapped = conversion == null
            && multiTypeEsField.getPotentiallyUnmappedExpression() != null
            && indexPage.mappedFields().contains(multiTypeEsField.getName()) == false;
        return isPotentiallyUnmapped ? (AbstractConvertFunction) multiTypeEsField.getPotentiallyUnmappedExpression() : conversion;
    }

    private IndexPage getIndexPage(DocBlock indexDoc) {
        return indexPages.get(indexDoc.asVector().shards().getInt(0));
    }

    private static Block getNullsBlock(DocBlock indexDoc) {
        return indexDoc.blockFactory().newConstantNullBlock(indexDoc.getPositionCount());
    }

    private sealed interface BlockResult {}

    private record BlockResultSuccess(Block block) implements BlockResult {}

    private record BlockResultMissing(String columnName, List<String> columnNames) implements BlockResult {}

    private BlockResult extractBlockForSingleDoc(DocBlock docBlock, String columnName, TestBlockCopier blockCopier) {
        var indexId = docBlock.asVector().shards().getInt(0);
        var indexPage = indexPages.get(indexId);
        return switch (columnName) {
            case MetadataAttribute.INDEX -> new BlockResultSuccess(
                docBlock.blockFactory()
                    .newConstantBytesRefBlockWith(new BytesRef(indexPage.index), blockCopier.docIndices.getPositionCount())
            );
            case DataTierFieldMapper.NAME -> new BlockResultSuccess(
                docBlock.blockFactory()
                    .newConstantBytesRefBlockWith(new BytesRef("data_content"), blockCopier.docIndices.getPositionCount())
            );
            default -> indexPage.columnIndex(columnName)
                .<BlockResult>map(columnIndex -> new BlockResultSuccess(blockCopier.copyBlock(indexPage.page.getBlock(columnIndex))))
                .orElseGet(() -> new BlockResultMissing(columnName, indexPage.columnNames()));
        };
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
            try (DocVector indexDocVector = vector.filter(false, currentList.stream().mapToInt(Integer::intValue).toArray())) {
                indexDocConsumer.accept(indexDocVector.asBlock());
            }
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
    private abstract static class TestSpatialShapeAbstractBlockCopier<T extends Block.Builder> extends TestBlockCopier {

        private TestSpatialShapeAbstractBlockCopier(IntVector docIndices) {
            super(docIndices);
        }

        protected abstract T blockBuilder(BytesRefBlock bytesRefBlock);

        protected abstract void initData();

        protected abstract void visitGeometry(Geometry geometry);

        protected abstract void encodeData(T builder);

        @Override
        protected Block copyBlock(Block originalData) {
            BytesRef scratch = new BytesRef(100);
            BytesRefBlock bytesRefBlock = (BytesRefBlock) originalData;
            try (T builder = blockBuilder(bytesRefBlock)) {
                for (int c = 0; c < docIndices.getPositionCount(); c++) {
                    int doc = docIndices.getInt(c);
                    int count = bytesRefBlock.getValueCount(doc);
                    if (count == 0) {
                        builder.appendNull();
                    } else {
                        initData();
                        int firstValueIndex = bytesRefBlock.getFirstValueIndex(doc);
                        for (int i = firstValueIndex; i < firstValueIndex + count; i++) {
                            BytesRef wkb = bytesRefBlock.getBytesRef(i, scratch);
                            Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                            visitGeometry(geometry);
                        }
                        encodeData(builder);
                    }
                }
                return builder.build();
            }
        }
    }

    private abstract static class TestSpatialShapeExtentBlockCopier extends TestSpatialShapeAbstractBlockCopier<IntBlock.Builder> {
        protected final SpatialEnvelopeVisitor.PointVisitor pointVisitor;
        private final SpatialEnvelopeVisitor visitor;

        private TestSpatialShapeExtentBlockCopier(IntVector docIndices, SpatialEnvelopeVisitor.PointVisitor pointVisitor) {
            super(docIndices);
            this.pointVisitor = pointVisitor;
            this.visitor = new SpatialEnvelopeVisitor(pointVisitor);
        }

        @Override
        protected IntBlock.Builder blockBuilder(BytesRefBlock bytesRefBlock) {
            return bytesRefBlock.blockFactory().newIntBlockBuilder(docIndices.getPositionCount());
        }

        @Override
        protected void initData() {
            pointVisitor.reset();
        }

        @Override
        protected void visitGeometry(Geometry geometry) {
            geometry.visit(visitor);
        }

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
            protected void encodeData(IntBlock.Builder builder) {
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
            protected void encodeData(IntBlock.Builder builder) {
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

    /**
     * geo_shape and cartesian_shape are normally loaded as WKT from source, but for ST_CENTROID_AGG we can load them from doc-values
     * extracting the centroid information. This class converts the test loaded WKB into the double[4] used in the aggregators.
     */
    private static class TestSpatialShapeCentroidBlockCopier extends TestSpatialShapeAbstractBlockCopier<DoubleBlock.Builder> {
        private final CoordinateEncoder encoder;
        private CentroidCalculator calculator;

        private TestSpatialShapeCentroidBlockCopier(IntVector docIndices, CoordinateEncoder encoder) {
            super(docIndices);
            this.encoder = encoder;
        }

        @Override
        protected DoubleBlock.Builder blockBuilder(BytesRefBlock bytesRefBlock) {
            return bytesRefBlock.blockFactory().newDoubleBlockBuilder(docIndices.getPositionCount());
        }

        @Override
        protected void initData() {
            calculator = new CentroidCalculator();
        }

        @Override
        protected void visitGeometry(Geometry geometry) {
            calculator.add(geometry);
        }

        @Override
        protected void encodeData(DoubleBlock.Builder builder) {
            builder.beginPositionEntry();
            builder.appendDouble(encoder.decodeX(encoder.encodeX(encoder.normalizeX(calculator.getX()))));
            builder.appendDouble(encoder.decodeY(encoder.encodeY(encoder.normalizeY(calculator.getY()))));
            builder.appendDouble(calculator.sumWeight());
            builder.appendDouble(calculator.getDimensionalShapeType().ordinal());
            builder.endPositionEntry();
        }

        private static TestSpatialShapeCentroidBlockCopier create(IntVector docIndices, DataType dataType) {
            return switch (dataType) {
                case GEO_SHAPE -> new TestSpatialShapeCentroidBlockCopier(docIndices, CoordinateEncoder.GEO);
                case CARTESIAN_SHAPE -> new TestSpatialShapeCentroidBlockCopier(docIndices, CoordinateEncoder.CARTESIAN);
                default -> throw new IllegalArgumentException("Unsupported spatial data type: " + dataType);
            };
        }
    }

    /**
     * geo_shape and cartesian_shape are normally loaded as WKT from source, but when both ST_EXTENT_AGG and ST_CENTROID_AGG are used
     * on the same field, we load combined bounds and centroid data from doc-values. This class converts the test loaded WKB into the
     * double[10] (geo) or double[8] (cartesian) used in the aggregators.
     */
    private abstract static class TestSpatialShapeBoundsAndCentroidBlockCopier extends TestSpatialShapeAbstractBlockCopier<
        DoubleBlock.Builder> {
        protected final SpatialEnvelopeVisitor.PointVisitor pointVisitor;
        private final SpatialEnvelopeVisitor visitor;
        private final CoordinateEncoder encoder;
        private CentroidCalculator calculator;

        private TestSpatialShapeBoundsAndCentroidBlockCopier(
            IntVector docIndices,
            SpatialEnvelopeVisitor.PointVisitor pointVisitor,
            CoordinateEncoder encoder
        ) {
            super(docIndices);
            this.pointVisitor = pointVisitor;
            this.visitor = new SpatialEnvelopeVisitor(pointVisitor);
            this.encoder = encoder;
        }

        @Override
        protected DoubleBlock.Builder blockBuilder(BytesRefBlock bytesRefBlock) {
            return bytesRefBlock.blockFactory().newDoubleBlockBuilder(docIndices.getPositionCount());
        }

        @Override
        protected void initData() {
            pointVisitor.reset();
            calculator = new CentroidCalculator();
        }

        @Override
        protected void visitGeometry(Geometry geometry) {
            geometry.visit(visitor);
            calculator.add(geometry);
        }

        @Override
        protected void encodeData(DoubleBlock.Builder builder) {
            builder.beginPositionEntry();
            encodeBounds(builder);
            encodeCentroid(builder, calculator);
            builder.endPositionEntry();
        }

        protected abstract void encodeBounds(DoubleBlock.Builder builder);

        private void encodeCentroid(DoubleBlock.Builder builder, CentroidCalculator calculator) {
            builder.appendDouble(encoder.decodeX(encoder.encodeX(encoder.normalizeX(calculator.getX()))));
            builder.appendDouble(encoder.decodeY(encoder.encodeY(encoder.normalizeY(calculator.getY()))));
            builder.appendDouble(calculator.sumWeight());
            builder.appendDouble(calculator.getDimensionalShapeType().ordinal());
        }

        private static TestSpatialShapeBoundsAndCentroidBlockCopier create(IntVector docIndices, DataType dataType) {
            return switch (dataType) {
                case GEO_SHAPE -> new TestGeoCopier(docIndices);
                case CARTESIAN_SHAPE -> new TestCartesianCopier(docIndices);
                default -> throw new IllegalArgumentException("Unsupported spatial data type: " + dataType);
            };
        }

        private static class TestGeoCopier extends TestSpatialShapeBoundsAndCentroidBlockCopier {
            private TestGeoCopier(IntVector docIndices) {
                super(
                    docIndices,
                    new SpatialEnvelopeVisitor.GeoPointVisitor(SpatialEnvelopeVisitor.WrapLongitude.WRAP),
                    CoordinateEncoder.GEO
                );
            }

            @Override
            protected void encodeBounds(DoubleBlock.Builder builder) {
                SpatialEnvelopeVisitor.GeoPointVisitor visitor = (SpatialEnvelopeVisitor.GeoPointVisitor) pointVisitor;
                builder.appendDouble(CoordinateEncoder.GEO.encodeY(visitor.getTop()));
                builder.appendDouble(CoordinateEncoder.GEO.encodeY(visitor.getBottom()));
                builder.appendDouble(encodeLongitude(visitor.getNegLeft()));
                builder.appendDouble(encodeLongitude(visitor.getNegRight()));
                builder.appendDouble(encodeLongitude(visitor.getPosLeft()));
                builder.appendDouble(encodeLongitude(visitor.getPosRight()));
            }
        }

        private static class TestCartesianCopier extends TestSpatialShapeBoundsAndCentroidBlockCopier {
            private TestCartesianCopier(IntVector docIndices) {
                super(docIndices, new SpatialEnvelopeVisitor.CartesianPointVisitor(), CoordinateEncoder.CARTESIAN);
            }

            @Override
            protected void encodeBounds(DoubleBlock.Builder builder) {
                SpatialEnvelopeVisitor.CartesianPointVisitor visitor = (SpatialEnvelopeVisitor.CartesianPointVisitor) pointVisitor;
                builder.appendDouble(CoordinateEncoder.CARTESIAN.encodeX(visitor.getMinX()));
                builder.appendDouble(CoordinateEncoder.CARTESIAN.encodeX(visitor.getMaxX()));
                builder.appendDouble(CoordinateEncoder.CARTESIAN.encodeY(visitor.getMaxY()));
                builder.appendDouble(CoordinateEncoder.CARTESIAN.encodeY(visitor.getMinY()));
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
        } else if ((extractPreference == EXTRACT_SPATIAL_CENTROID || extractPreference == EXTRACT_SPATIAL_BOUNDS_AND_CENTROID)
            && isShapeType(dataType)) {
                return blockFactory.newDoubleBlockBuilder(estimatedSize);
            } else {
                return elementType.newBlockBuilder(estimatedSize, blockFactory);
            }
    }

    private static TestBlockCopier blockCopier(DataType dataType, FieldExtractPreference extractPreference, IntVector docIndices) {
        if (extractPreference == DOC_VALUES && DataType.isSpatialPoint(dataType)) {
            return TestSpatialPointStatsBlockCopier.create(docIndices, dataType);
        } else if (extractPreference == EXTRACT_SPATIAL_BOUNDS && DataType.isSpatial(dataType)) {
            return TestSpatialShapeExtentBlockCopier.create(docIndices, dataType);
        } else if (extractPreference == EXTRACT_SPATIAL_CENTROID && isShapeType(dataType)) {
            return TestSpatialShapeCentroidBlockCopier.create(docIndices, dataType);
        } else if (extractPreference == EXTRACT_SPATIAL_BOUNDS_AND_CENTROID && isShapeType(dataType)) {
            return TestSpatialShapeBoundsAndCentroidBlockCopier.create(docIndices, dataType);
        } else {
            return new TestBlockCopier(docIndices);
        }
    }

    private static boolean isShapeType(DataType dataType) {
        return dataType == DataType.GEO_SHAPE || dataType == DataType.CARTESIAN_SHAPE;
    }
}
