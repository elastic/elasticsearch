/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.LuceneCountOperator;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.query.MinCompetitiveQuery;
import org.elasticsearch.compute.lucene.query.TimeSeriesSourceOperator;
import org.elasticsearch.compute.lucene.read.ReadDimsOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.BlockLoaderWarnings;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.Sort;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ReadDimsExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter;
import static org.elasticsearch.compute.lucene.query.LuceneSourceOperator.NO_LIMIT;
import static org.elasticsearch.index.get.ShardGetService.maybeExcludeVectorFields;

public class EsPhysicalOperationProviders extends AbstractPhysicalOperationProviders {
    private static final Logger logger = LogManager.getLogger(EsPhysicalOperationProviders.class);

    /**
     * Context of each shard we're operating against. Note these objects are shared across multiple operators as
     * {@link RefCounted}.
     */
    public abstract static class ShardContext implements org.elasticsearch.compute.lucene.ShardContext, Releasable {
        private final AbstractRefCounted refCounted = new AbstractRefCounted() {
            @Override
            protected void closeInternal() {
                ShardContext.this.close();
            }
        };

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        /**
         * Convert a {@link QueryBuilder} into a real {@link Query lucene query}.
         */
        public abstract Query toQuery(QueryBuilder queryBuilder);

        public abstract IndexSettings indexSettings();

        public abstract MappingLookup mappingLookup();

        /**
         * Tuning parameter for deciding when to use the "merge" stored field loader.
         * Think of it as "how similar to a sequential block of documents do I have to
         * be before I'll use the merge reader?" So a value of {@code 1} means I have to
         * be <strong>exactly</strong> a sequential block, like {@code 0, 1, 2, 3, .. 1299, 1300}.
         * A value of {@code .2} means we'll use the sequential reader even if we only
         * need one in ten documents.
         */
        public abstract double storedFieldsSequentialProportion();

        /**
         * Returns {@code true} if {@code name} is a concrete mapped field in this shard's
         * index — including index-level runtime fields and dynamically-resolved sub-fields of
         * {@link MetadataFieldMapper} instances, but excluding dynamically-resolved sub-keys of
         * {@code flattened} fields. Those sub-keys appear in Lucene's field infos even though they
         * are absent from the mapping, and counting them with an EXISTS query would inflate
         * aggregation results when field types differ across indices.
         * <p>
         * The default implementation uses the mapping lookup alone. {@link DefaultShardContext}
         * overrides this to also respect query-time runtime fields and {@code allowedFields}.
         */
        public boolean isMappedField(String name) {
            MappingLookup lookup = mappingLookup();
            if (lookup.getFullNameToFieldType().containsKey(name)) {
                return true;
            }
            int dotIndex = name.indexOf('.');
            if (dotIndex > 0) {
                return lookup.getMapper(name.substring(0, dotIndex)) instanceof MetadataFieldMapper metaMapper
                    && metaMapper.fieldType() instanceof DynamicFieldType dft
                    && dft.getChildFieldType(name.substring(dotIndex + 1)) != null;
            }
            return false;
        }
    }

    private final IndexedByShardId<? extends ShardContext> shardContexts;

    private final PlannerSettings plannerSettings;

    private final LongSupplier directoryBytesRead;

    private final QueryWarnings singleValueQueryWarnings;

    public EsPhysicalOperationProviders(
        FoldContext foldContext,
        IndexedByShardId<? extends ShardContext> shardContexts,
        AnalysisRegistry analysisRegistry,
        PlannerSettings plannerSettings,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(foldContext, analysisRegistry);
        this.shardContexts = shardContexts;
        this.plannerSettings = plannerSettings;
        this.directoryBytesRead = directoryBytesRead;
        this.singleValueQueryWarnings = singleValueQueryWarnings;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(
        FieldExtractExec fieldExtractExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        Layout.Builder layout = source.layout.builder();
        var sourceAttr = fieldExtractExec.sourceAttribute();
        int docChannel = source.layout.get(sourceAttr.id()).channel();
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.append(attr);
        }
        var fields = extractFields(fieldExtractExec);
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readers = shardContexts.map(
            s -> new ValuesSourceReaderOperator.ShardContext(
                s.searcher().getIndexReader(),
                s::newSourceLoader,
                s.storedFieldsSequentialProportion()
            )
        );
        boolean reuseColumnLoaders = fieldExtractExec.attributesToExtract().size() <= plannerSettings.reuseColumnLoadersThreshold();
        int docSequenceThreshold = context.queryPragmas()
            .docSequenceBytesRefFieldThreshold(plannerSettings.docSequenceBytesRefFieldThreshold());
        return source.with(
            new ValuesSourceReaderOperator.Factory(
                plannerSettings.valuesLoadingJumboSize(),
                fields,
                readers,
                reuseColumnLoaders,
                docChannel,
                plannerSettings.sourceReservationFactor(),
                docSequenceThreshold,
                directoryBytesRead
            ),
            layout.build()
        );
    }

    @Override
    public PhysicalOperation readDimsPhysicalOperation(
        ReadDimsExec readDimsExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        var fields = extractFields(readDimsExec, readDimsExec.dims(), f -> readDimsExec.fieldExtractPreference());
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readers = shardContexts.map(
            s -> new ValuesSourceReaderOperator.ShardContext(
                s.searcher().getIndexReader(),
                s::newSourceLoader,
                s.storedFieldsSequentialProportion()
            )
        );
        ValuesSourceReaderOperator.Factory valuesSourceReader = new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofBytes(Long.MAX_VALUE), // aggs are chunked already
            fields,
            readers,
            readDimsExec.dims().size() <= plannerSettings.reuseColumnLoadersThreshold(),
            0, // delegate a single doc-block to the reader
            plannerSettings.sourceReservationFactor(),
            context.queryPragmas().docSequenceBytesRefFieldThreshold(plannerSettings.docSequenceBytesRefFieldThreshold()),
            directoryBytesRead
        );
        int docChannel = source.layout.get(readDimsExec.docAttribute().id()).channel();
        int tsidChannel = source.layout.get(readDimsExec.tsidAttribute().id()).channel();
        Layout.Builder layout = source.layout.builder();
        for (Attribute attr : readDimsExec.dims()) {
            layout.append(attr);
        }
        return source.with(new ReadDimsOperator.Factory(valuesSourceReader, docChannel, tsidChannel), layout.build());
    }

    private static String getFieldName(Attribute attr) {
        // Do not use the field attribute name, this can deviate from the field name for union types.
        return attr instanceof FieldAttribute fa ? fa.fieldName().string() : attr.name();
    }

    private ValuesSourceReaderOperator.LoaderAndConverter blockLoaderAndConverter(
        DriverContext driverContext,
        int shardId,
        Attribute attr,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        DefaultShardContext shardContext = (DefaultShardContext) shardContexts.get(shardId);
        if (attr instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
            shardContext = wrapWithUnmappedFieldContext(shardContext, getFieldName(fa));
        }
        if (attr instanceof UnmappedFieldsAttribute ufa) {
            // The pattern's excludes cover what field caps reported to the coordinator, not this shard's mapping, so a field mapped
            // here but missing there - a dynamic mapping update that landed after resolution - is read out of _source and reported
            // as unmapped. LOAD has the same race, where it instead loads the field with its new type into a column the coordinator
            // already declared keyword, so both modes are consistent in planning against the schema as of resolution time.
            return ValuesSourceReaderOperator.load(new UnmappedFieldsBlockLoader(ufa.pattern(), plannerSettings.sourceReservationFactor()));
        }

        // Apply any block loader function if present

        BlockLoaderFunctionConfig functionConfig = null;
        BlockLoaderWarnings warnings = new BlockLoaderWarnings(driverContext, attr.source());
        String fieldName = getFieldName(attr);
        if (attr instanceof TimeSeriesMetadataAttribute timeSeriesMetadataAttribute) {
            functionConfig = new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, timeSeriesMetadataAttribute.excludedFields());
            fieldName = SourceFieldMapper.NAME;
        } else if (attr instanceof TemporalityAttribute) {
            return resolveTemporalitySource(shardContext, warnings, fieldExtractPreference);
        } else if (attr instanceof FieldAttribute fieldAttr && fieldAttr.field() instanceof FunctionEsField functionEsField) {
            functionConfig = functionEsField.functionConfig();
        }
        boolean isUnsupported = attr.dataType() == DataType.UNSUPPORTED;
        UnionTypeEsField unionTypes = findUnionTypes(attr);
        if (unionTypes == null) {
            BlockLoader blockLoader = shardContext.blockLoader(
                fieldName,
                isUnsupported,
                fieldExtractPreference,
                functionConfig,
                warnings,
                plannerSettings.blockLoaderSizeOrdinals(),
                plannerSettings.blockLoaderSizeScript()
            );
            return ValuesSourceReaderOperator.load(blockLoader);
        }
        Expression conversion = switch (unionTypes) {
            case CompactMultiTypeEsField compact -> {
                MappedFieldType mft = shardContext.fieldType(fieldName);
                // Match what field_caps reports on the coordinator: family type (e.g., constant_keyword -> keyword) rather than the
                // concrete mapper type, so the lookup key here aligns with how typeToConversionExpressions was keyed upstream.
                yield mft == null
                    ? null
                    : compact.getConversionExpressionForType(
                        EsqlDataTypeRegistry.INSTANCE.fromEs(mft.familyTypeName(), mft.getMetricType())
                    );
            }
            case MultiTypeEsField legacy -> {
                // Use the fully qualified name `cluster:index-name` because multiple types are resolved on coordinator with cluster prefix
                String indexName = shardContext.ctx.getFullyQualifiedIndex().getName();
                yield legacy.getConversionExpressionForIndex(indexName);
            }
        };
        if (conversion == null) {
            Expression potentiallyUnmapped = unionTypes.getUnmappedConversionExpression();
            if (!(potentiallyUnmapped instanceof AbstractConvertFunction convert)) {
                return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
            }
            fieldName = getFieldName((Attribute) convert.field());
            shardContext = wrapWithUnmappedFieldContext(shardContext, fieldName);
            conversion = potentiallyUnmapped;
        }
        if (conversion instanceof BlockLoaderExpression ble) {
            BlockLoaderExpression.PushedBlockLoaderExpression e = ble.tryPushToFieldLoading(SearchStats.EMPTY);
            if (e != null) {
                return ValuesSourceReaderOperator.load(
                    shardContext.blockLoader(
                        fieldName,
                        isUnsupported,
                        fieldExtractPreference,
                        e.config(),
                        warnings,
                        plannerSettings.blockLoaderSizeOrdinals(),
                        plannerSettings.blockLoaderSizeScript()
                    )
                );
            }
        }
        BlockLoader blockLoader = shardContext.blockLoader(
            fieldName,
            isUnsupported,
            fieldExtractPreference,
            functionConfig,
            warnings,
            plannerSettings.blockLoaderSizeOrdinals(),
            plannerSettings.blockLoaderSizeScript()
        );
        return ValuesSourceReaderOperator.loadAndConvert(blockLoader, new TypeConverter((EsqlScalarFunction) conversion));
    }

    private ValuesSourceReaderOperator.LoaderAndConverter resolveTemporalitySource(
        DefaultShardContext shardContext,
        BlockLoaderWarnings warnings,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        Settings indexSettings = shardContext.indexSettings().getSettings();
        String temporalityFieldName = IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.get(indexSettings);
        if (temporalityFieldName == null || temporalityFieldName.isEmpty()) {
            // index does not have a temporality field configured, return constant nulls
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        MappedFieldType temporalityFieldType = shardContext.fieldType(temporalityFieldName);
        if (temporalityFieldType == null) {
            // configured field does not exist in this index, return constant nulls
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        if (KeywordFieldMapper.CONTENT_TYPE.equals(temporalityFieldType.typeName()) == false) {
            warnings.registerException(
                IllegalArgumentException.class,
                "configured temporality field ["
                    + temporalityFieldName
                    + "] has type ["
                    + temporalityFieldType.typeName()
                    + "], expected ["
                    + KeywordFieldMapper.CONTENT_TYPE
                    + "]; assuming default temporality for all values"
            );
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        if (temporalityFieldType.isDimension() == false) {
            warnings.registerException(
                IllegalArgumentException.class,
                "configured temporality field ["
                    + temporalityFieldName
                    + "] must be a time-series dimension; assuming default temporality for all values"
            );
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        return ValuesSourceReaderOperator.load(
            shardContext.blockLoader(
                temporalityFieldName,
                false,
                fieldExtractPreference,
                null,
                warnings,
                plannerSettings.blockLoaderSizeOrdinals(),
                plannerSettings.blockLoaderSizeScript()
            )
        );
    }

    static DefaultShardContext wrapWithUnmappedFieldContext(DefaultShardContext ctx, String fullFieldName) {
        return new DefaultShardContextForUnmappedField(ctx, fullFieldName);
    }

    /** A hack to pretend an unmapped field still exists. */
    private static class DefaultShardContextForUnmappedField extends DefaultShardContext {
        private static final FieldType UNMAPPED_FIELD_TYPE = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        static {
            UNMAPPED_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            UNMAPPED_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            UNMAPPED_FIELD_TYPE.setStored(false);
            UNMAPPED_FIELD_TYPE.freeze();
        }
        private final String fullFieldName;

        DefaultShardContextForUnmappedField(DefaultShardContext ctx, String fullFieldName) {
            super(ctx.index, ctx.releasable, ctx.ctx, ctx.aliasFilter);
            this.fullFieldName = fullFieldName;
        }

        @Override
        public boolean isMappedField(String name) {
            // For the unmapped field we are loading, bypass the mapped-field gate.
            // This allows both truly unmapped fields (which use a source-based loader) and
            // dynamic subfields of flattened fields (which use the keyed block loader) to
            // produce real values rather than ConstantNull.
            return name.equals(fullFieldName) || super.isMappedField(name);
        }

        @Override
        public @Nullable MappedFieldType fieldType(String name) {
            var superResult = super.fieldType(name);
            return superResult == null && name.equals(fullFieldName) ? createUnmappedFieldType(name, this) : superResult;
        }

        static MappedFieldType createUnmappedFieldType(String name, DefaultShardContext context) {
            var builder = new KeywordFieldMapper.Builder(name, context.ctx.getIndexSettings());
            builder.docValues(false);
            builder.indexed(false);
            return new KeywordFieldMapper.KeywordFieldType(
                name,
                IndexType.terms(false, false),
                new TextSearchInfo(UNMAPPED_FIELD_TYPE, builder.similarity(), Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Lucene.KEYWORD_ANALYZER,
                builder,
                context.ctx.isSourceSynthetic()
            );
        }
    }

    private static @Nullable UnionTypeEsField findUnionTypes(Attribute attr) {
        if (attr instanceof FieldAttribute fa && fa.field() instanceof UnionTypeEsField unionTypeEsField) {
            return unionTypeEsField;
        }
        return null;
    }

    public Function<org.elasticsearch.compute.lucene.ShardContext, List<LuceneSliceQueue.QueryAndTags>> querySupplier(
        QueryBuilder builder
    ) {
        QueryBuilder qb = builder == null ? QueryBuilders.matchAllQuery().boost(0.0f) : builder;
        return ctx -> List.of(new LuceneSliceQueue.QueryAndTags(shardContexts.get(ctx.index()).toQuery(qb), List.of()));
    }

    public Function<org.elasticsearch.compute.lucene.ShardContext, List<LuceneSliceQueue.QueryAndTags>> querySupplier(
        List<EsQueryExec.QueryBuilderAndTags> queryAndTagsFromEsQueryExec
    ) {
        return ctx -> queryAndTagsFromEsQueryExec.stream().map(queryBuilderAndTags -> {
            QueryBuilder qb = queryBuilderAndTags.query();
            return new LuceneSliceQueue.QueryAndTags(
                shardContexts.get(ctx.index()).toQuery(qb == null ? QueryBuilders.matchAllQuery().boost(0.0f) : qb),
                queryBuilderAndTags.tags()
            );
        }).toList();
    }

    /**
     * Like {@link #querySupplier(QueryBuilder)} but skips shards where {@code fieldName} is not
     * a concrete mapped field. Flattened fields store terms for their sub-keys in Lucene even though
     * those sub-keys are absent from the real mapping; a plain EXISTS query would therefore find
     * documents in flattened shards and inflate field-level COUNT results. Wildcard ({@code "*"})
     * means COUNT(*) — count every document — so no per-field guard is applied in that case.
     */
    public Function<org.elasticsearch.compute.lucene.ShardContext, List<LuceneSliceQueue.QueryAndTags>> querySupplierForField(
        QueryBuilder builder,
        String fieldName
    ) {
        Function<org.elasticsearch.compute.lucene.ShardContext, List<LuceneSliceQueue.QueryAndTags>> innerFn = querySupplier(builder);
        if ("*".equals(fieldName)) {
            return innerFn;
        }
        return ctx -> {
            if (shardContexts.get(ctx.index()).isMappedField(fieldName) == false) {
                return List.of();
            }
            return innerFn.apply(ctx);
        };
    }

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        final LuceneOperator.Factory luceneFactory;
        logger.trace("Query Exec is {}", esQueryExec);

        List<Sort> sorts = esQueryExec.sorts();
        assert esQueryExec.estimatedRowSize() != null : "estimated row size not initialized";
        int rowEstimatedSize = esQueryExec.estimatedRowSize();
        int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold(context.foldCtx()) : NO_LIMIT;
        boolean scoring = esQueryExec.hasScoring();
        int taskConcurrency = context.queryPragmas().taskConcurrency();
        if (context.timeSeries()) {
            // Time-series aggregation is CPU-bound and cache-sensitive; cap concurrency at the number of processors.
            taskConcurrency = Math.min(taskConcurrency, Math.max(EsExecutors.allocatedProcessors(context.settings()), 2));
        }
        if (sorts != null && sorts.isEmpty() == false) {
            List<SortBuilder<?>> sortBuilders = new ArrayList<>(sorts.size());
            long estimatedPerRowSortSize = 0;
            for (Sort sort : sorts) {
                sortBuilders.add(sort.sortBuilder());
                estimatedPerRowSortSize += EstimatesRowSize.estimateSize(sort.resulType());
            }
            /*
             * In the worst case Lucene's TopN keeps each value in memory twice. Once
             * for the actual sort and once for the top doc. In the best case they share
             * references to the same underlying data, but we're being a bit paranoid here.
             */
            estimatedPerRowSortSize *= 2;
            // LuceneTopNSourceOperator does not support QueryAndTags, if there are multiple queries or if the single query has tags,
            // UnsupportedOperationException will be thrown by esQueryExec.query()
            luceneFactory = new LuceneTopNSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.query()),
                context.queryPragmas().dataPartitioning(plannerSettings.defaultDataPartitioning()),
                LuceneTopNSourceOperator.autoStrategy(
                    sortBuilders,
                    scoring,
                    context.queryPragmas().minDocsPerSlice(LuceneSliceQueue.MIN_DOCS_PER_SLICE)
                ),
                taskConcurrency,
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit,
                sortBuilders,
                estimatedPerRowSortSize,
                scoring,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        } else if (esQueryExec.indexMode().isTsdb()) {
            luceneFactory = new TimeSeriesSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.queryBuilderAndTags()),
                context.queryPragmas().dataPartitioning(plannerSettings.defaultDataPartitioning()),
                context.queryPragmas().docsThresholdForAutoPartitioning(plannerSettings.docsThresholdForAutoPartitioning()),
                taskConcurrency,
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        } else {
            // A no-limit scan (e.g. STATS) visits every matching doc, so it shares the cost-threshold rule with count and
            // TopN: cheap -> SEGMENT, scan-heavy -> DOC (implicit-limit stays SHARD; time-series keeps its own strategy).
            long minCostForDoc = context.queryPragmas().minDocsPerSlice(LuceneSliceQueue.MIN_DOCS_PER_SLICE);
            var autoStrategy = context.timeSeries()
                ? context.autoPartitioningStrategy()
                : LuceneSourceOperator.Factory.autoStrategy(minCostForDoc);
            LuceneMinCompetitivePilot minCompetitivePilot = context.luceneMinCompetitivePilot().get();
            LuceneSliceQueue.SlicePriority slicePriority = null;
            if (minCompetitivePilot != null && plannerSettings.minCompetitiveSliceOrderByTimestampEnabled()) {
                slicePriority = new LuceneSliceQueue.SlicePriority(minCompetitivePilot.sortFieldName(), true);
            }
            luceneFactory = new LuceneSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.queryBuilderAndTags()),
                context.queryPragmas().dataPartitioning(plannerSettings.defaultDataPartitioning()),
                autoStrategy,
                context.queryPragmas().docsThresholdForAutoPartitioning(plannerSettings.docsThresholdForAutoPartitioning()),
                taskConcurrency,
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit,
                scoring,
                directoryBytesRead,
                context.queryPragmas().minDocsPerSlice(LuceneSliceQueue.MIN_DOCS_PER_SLICE),
                singleValueQueryWarnings,
                planMinCompetitive(minCompetitivePilot),
                slicePriority
            );
        }
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    List<ValuesSourceReaderOperator.FieldInfo> extractFields(FieldExtractExec fieldExtractExec) {
        return extractFields(fieldExtractExec, fieldExtractExec.attributesToExtract(), fieldExtractExec::fieldExtractPreference);
    }

    List<ValuesSourceReaderOperator.FieldInfo> extractFields(
        PhysicalPlan planForNullsFilter,
        List<Attribute> attributes,
        Function<Attribute, MappedFieldType.FieldExtractPreference> fieldExtractPreference
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(attributes.size());
        Set<String> nullsFilteredFields = new HashSet<>();
        planForNullsFilter.forEachDown(EsQueryExec.class, queryExec -> {
            QueryBuilder q = queryExec.queryBuilderAndTags().get(0).query();
            if (q != null) {
                nullsFilteredFields.addAll(nullsFilteredFieldsAfterSourceQuery(q));
            }
        });
        for (Attribute attr : attributes) {
            DataType dataType = attr.dataType();
            var preference = fieldExtractPreference.apply(attr);
            ElementType elementType = PlannerUtils.toElementType(dataType, preference);
            ValuesSourceReaderOperator.BuildLoader buildLoader = (driverContext, s) -> blockLoaderAndConverter(
                driverContext,
                s,
                attr,
                preference
            );
            String fieldName = getFieldName(attr);
            boolean nullsFiltered = nullsFilteredFields.contains(fieldName);
            fieldInfos.add(new ValuesSourceReaderOperator.FieldInfo(fieldName, elementType, nullsFiltered, buildLoader));
        }
        return fieldInfos;
    }

    private MinCompetitiveQuery.Factory planMinCompetitive(@Nullable LuceneMinCompetitivePilot pilot) {
        if (pilot == null) {
            return null;
        }
        MinCompetitiveQuery.BuildMinCompetitiveQuery buildMinCompetitiveQuery = (ctx, page) -> {
            SearchExecutionContext executionContext = ((DefaultShardContext) ctx).ctx;
            EsMinCompetitiveQueries minCompetitiveQueries = new EsMinCompetitiveQueries(
                pilot.supplier(),
                pilot.sortFieldName(),
                executionContext
            );
            Query q = minCompetitiveQueries.buildMinCompetitiveQuery(page);
            return q.rewrite(executionContext.searcher());
        };
        MinCompetitiveQuery.BuildPrimaryFilterQuery buildPrimaryFilterQuery = ctx -> {
            SearchExecutionContext executionContext = ((DefaultShardContext) ctx).ctx;
            EsMinCompetitiveQueries minCompetitiveQueries = new EsMinCompetitiveQueries(
                pilot.supplier(),
                pilot.sortFieldName(),
                executionContext
            );
            Query q = minCompetitiveQueries.buildMissingTimestampPrimaryFilter();
            return q.rewrite(executionContext.searcher());
        };
        return new MinCompetitiveQuery.Factory(pilot.supplier(), buildMinCompetitiveQuery, buildPrimaryFilterQuery);
    }

    /**
     * Returns the set of fields that are guaranteed to be dense after the source query.
     */
    static Set<String> nullsFilteredFieldsAfterSourceQuery(QueryBuilder sourceQuery) {
        return switch (sourceQuery) {
            case ExistsQueryBuilder q -> Set.of(q.fieldName());
            case TermQueryBuilder q -> Set.of(q.fieldName());
            case TermsQueryBuilder q -> Set.of(q.fieldName());
            case RangeQueryBuilder q -> Set.of(q.fieldName());
            case ConstantScoreQueryBuilder q -> nullsFilteredFieldsAfterSourceQuery(q.innerQuery());
            // TODO: support SingleValueQuery
            case BoolQueryBuilder q -> {
                final Set<String> fields = new HashSet<>();
                for (List<QueryBuilder> clauses : List.of(q.must(), q.filter())) {
                    for (QueryBuilder c : clauses) {
                        fields.addAll(nullsFilteredFieldsAfterSourceQuery(c));
                    }
                }
                // safe to ignore must_not and should clauses
                yield fields;
            }
            default -> Set.of();
        };
    }

    /**
     * Build a {@link SourceOperator.SourceOperatorFactory} that counts documents in the search index.
     */
    public LuceneCountOperator.Factory countSource(
        LocalExecutionPlannerContext context,
        Function<org.elasticsearch.compute.lucene.ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
        List<ElementType> tagTypes,
        Expression limit
    ) {
        return new LuceneCountOperator.Factory(
            shardContexts,
            queryFunction,
            context.queryPragmas().dataPartitioning(plannerSettings.defaultDataPartitioning()),
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            context.queryPragmas().taskConcurrency(),
            tagTypes,
            limit == null ? NO_LIMIT : (Integer) limit.fold(context.foldCtx()),
            directoryBytesRead,
            context.queryPragmas().minDocsPerSlice(LuceneSliceQueue.MIN_DOCS_PER_SLICE),
            singleValueQueryWarnings
        );
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
        var pragmas = context.queryPragmas();
        int targetChunkRows = pragmas.timeSeriesTargetChunkRows(plannerSettings.timeSeriesTargetChunkRows());
        return new TimeSeriesAggregationOperator.Factory(
            ts.timeBucketRounding(context.foldCtx()),
            ts.timeBucket() != null && ts.timeBucket().dataType() == DataType.DATE_NANOS,
            groupSpecs,
            aggregatorMode,
            aggregatorFactories,
            context.pageSize(ts, ts.estimatedRowSize()),
            targetChunkRows
        );
    }

    public static class DefaultShardContext extends ShardContext {
        private final int index;

        /**
         * In production, this will be a {@link SearchContext}, but we don't want to drag that huge
         * dependency here.
         */
        private final Releasable releasable;
        private final SearchExecutionContext ctx;
        private final AliasFilter aliasFilter;
        private final String shardIdentifier;
        private final ShardSearchStats shardSearchStats;

        public DefaultShardContext(int index, Releasable releasable, SearchExecutionContext ctx, AliasFilter aliasFilter) {
            this.index = index;
            this.releasable = releasable;
            this.ctx = ctx;
            this.aliasFilter = aliasFilter;
            // Build the shardIdentifier once up front so we can reuse references to it in many places.
            this.shardIdentifier = this.ctx.getFullyQualifiedIndex().getName() + ":" + this.ctx.getShardId();
            this.shardSearchStats = ctx.stats();
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public IndexSearcher searcher() {
            return ctx.searcher();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) throws IOException {
            return SortBuilder.buildSort(sorts, ctx, false);
        }

        @Override
        public String shardIdentifier() {
            return shardIdentifier;
        }

        @Override
        public SourceLoader newSourceLoader(Set<String> sourcePaths) {
            var filter = buildSourceFilter(sourcePaths, ctx.getMappingLookup(), ctx.getIndexSettings());
            return ctx.newSourceLoader(filter, false);
        }

        static SourceFilter buildSourceFilter(Set<String> sourcePaths, MappingLookup mappingLookup, IndexSettings indexSettings) {
            var filter = sourcePaths != null ? new SourceFilter(sourcePaths.toArray(new String[0]), null) : null;
            // Apply vector exclusion logic similar to ShardGetService
            var fetchSourceContext = filter != null ? FetchSourceContext.of(true, null, filter.getIncludes(), filter.getExcludes()) : null;
            var result = maybeExcludeVectorFields(mappingLookup, indexSettings, fetchSourceContext, null);
            var vectorFilter = result.v2();
            if (vectorFilter != null) {
                var includes = filter != null ? filter.getIncludes() : new String[0];
                filter = new SourceFilter(includes, vectorFilter.getExcludes());
            }
            return filter;
        }

        @Override
        public Query toQuery(QueryBuilder queryBuilder) {
            Query query = ctx.toQuery(queryBuilder).query();
            if (ctx.nestedLookup() != NestedLookup.EMPTY && NestedHelper.mightMatchNestedDocs(query, ctx)) {
                // filter out nested documents
                query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                    .add(newNonNestedFilter(ctx.indexVersionCreated()), BooleanClause.Occur.FILTER)
                    .build();
            }
            if (aliasFilter != AliasFilter.EMPTY) {
                Query filterQuery = ctx.toQuery(aliasFilter.getQueryBuilder()).query();
                query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                    .add(filterQuery, BooleanClause.Occur.FILTER)
                    .build();
            }
            return query;
        }

        @Override
        public IndexSettings indexSettings() {
            return ctx.getIndexSettings();
        }

        @Override
        public MappingLookup mappingLookup() {
            return ctx.getMappingLookup();
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference,
            BlockLoaderFunctionConfig blockLoaderFunctionConfig,
            org.elasticsearch.index.mapper.blockloader.Warnings warnings,
            ByteSizeValue blockLoaderSizeOrdinals,
            ByteSizeValue blockLoaderSizeScript
        ) {
            if (asUnsupportedSource) {
                return ConstantNull.INSTANCE;
            }
            // Resolve the field type in a single pass. fieldType() (via the search execution context) applies field-level
            // security, resolves slice aliases (e.g. _slice -> _routing) and query-time runtime fields, and returns null for
            // fields absent from this context.
            MappedFieldType fieldType = fieldType(name);
            if (fieldType == null) {
                // the field does not exist in this context
                return ConstantNull.INSTANCE;
            }
            // Exclude dynamically-resolved flattened sub-keys: fieldType() resolves them to a non-null type, but field caps
            // does not report them and they must not be extracted (see #154508). Only a dotted name can be such a sub-key,
            // and for a flat name a non-null fieldType already implies isMappedField(name) == true — so gating the (virtual)
            // mapped-field probe on the dot keeps flat names (the common case) at a single resolution.
            if (name.indexOf('.') > 0 // only dotted names can be flattened sub-keys; skip the redundant probe for flat names
                && isMappedField(name) == false) {
                return ConstantNull.INSTANCE;
            }
            BlockLoader loader = fieldType.blockLoader(
                new EsqlBlockLoaderContext(
                    ctx,
                    fieldExtractPreference,
                    blockLoaderFunctionConfig,
                    warnings,
                    blockLoaderSizeOrdinals,
                    blockLoaderSizeScript
                )
            );
            if (loader == null) {
                // Compute-time warning: queued for the later warnings-sink fix. Not routed through the in-scope
                // blockloader `warnings` object because its only method, registerException(Class, String), would
                // reframe this plain message as a located exception-style warning, changing the emitted content.
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", name);
                return ConstantNull.INSTANCE;
            }

            return loader;
        }

        @Override
        public @Nullable MappedFieldType fieldType(String name) {
            return ctx.getFieldType(name);
        }

        @Override
        public ShardSearchStats stats() {
            return shardSearchStats;
        }

        @Override
        public double storedFieldsSequentialProportion() {
            return EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.get(ctx.getIndexSettings().getSettings());
        }

        @Override
        public boolean isMappedField(String name) {
            return ctx.isMappedField(name);
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
