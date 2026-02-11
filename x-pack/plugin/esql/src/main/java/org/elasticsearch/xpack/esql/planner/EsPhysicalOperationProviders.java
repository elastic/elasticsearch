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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.LuceneCountOperator;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.TimeSeriesSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
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
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.Sort;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter;
import static org.elasticsearch.compute.lucene.LuceneSourceOperator.NO_LIMIT;
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

        /**
         * Tuning parameter for deciding when to use the "merge" stored field loader.
         * Think of it as "how similar to a sequential block of documents do I have to
         * be before I'll use the merge reader?" So a value of {@code 1} means I have to
         * be <strong>exactly</strong> a sequential block, like {@code 0, 1, 2, 3, .. 1299, 1300}.
         * A value of {@code .2} means we'll use the sequential reader even if we only
         * need one in ten documents.
         */
        public abstract double storedFieldsSequentialProportion();
    }

    private final IndexedByShardId<? extends ShardContext> shardContexts;
    private final PlannerSettings plannerSettings;

    public EsPhysicalOperationProviders(
        FoldContext foldContext,
        IndexedByShardId<? extends ShardContext> shardContexts,
        AnalysisRegistry analysisRegistry,
        PlannerSettings plannerSettings
    ) {
        super(foldContext, analysisRegistry);
        this.shardContexts = shardContexts;
        this.plannerSettings = plannerSettings;
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
        boolean reuseColumnLoaders = fieldExtractExec.attributesToExtract().size() <= context.plannerSettings()
            .reuseColumnLoadersThreshold();
        return source.with(
            new ValuesSourceReaderOperator.Factory(
                plannerSettings.valuesLoadingJumboSize(),
                fields,
                readers,
                reuseColumnLoaders,
                docChannel
            ),
            layout.build()
        );
    }

    private static String getFieldName(Attribute attr) {
        // Do not use the field attribute name, this can deviate from the field name for union types.
        return attr instanceof FieldAttribute fa ? fa.fieldName().string() : attr.name();
    }

    private ValuesSourceReaderOperator.LoaderAndConverter blockLoaderAndConverter(
        int shardId,
        Attribute attr,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        DefaultShardContext shardContext = (DefaultShardContext) shardContexts.get(shardId);
        if (attr instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField kf) {
            shardContext = new DefaultShardContextForUnmappedField(shardContext, kf);
        }

        // Apply any block loader function if present
        BlockLoaderFunctionConfig functionConfig = null;
        if (attr instanceof FieldAttribute fieldAttr && fieldAttr.field() instanceof FunctionEsField functionEsField) {
            functionConfig = functionEsField.functionConfig();
        }
        boolean isUnsupported = attr.dataType() == DataType.UNSUPPORTED;
        String fieldName = getFieldName(attr);
        BlockLoader blockLoader = shardContext.blockLoader(fieldName, isUnsupported, fieldExtractPreference, functionConfig);
        MultiTypeEsField unionTypes = findUnionTypes(attr);
        if (unionTypes == null) {
            return ValuesSourceReaderOperator.load(blockLoader);
        }
        // Use the fully qualified name `cluster:index-name` because multiple types are resolved on coordinator with the cluster prefix
        String indexName = shardContext.ctx.getFullyQualifiedIndex().getName();
        Expression conversion = unionTypes.getConversionExpressionForIndex(indexName);
        if (conversion == null) {
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        if (conversion instanceof BlockLoaderExpression ble) {
            BlockLoaderExpression.PushedBlockLoaderExpression e = ble.tryPushToFieldLoading(SearchStats.EMPTY);
            if (e != null) {
                return ValuesSourceReaderOperator.load(
                    shardContext.blockLoader(fieldName, isUnsupported, fieldExtractPreference, e.config())
                );
            }
        }
        return ValuesSourceReaderOperator.loadAndConvert(blockLoader, new TypeConverter((EsqlScalarFunction) conversion));
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
        private final KeywordEsField unmappedEsField;

        DefaultShardContextForUnmappedField(DefaultShardContext ctx, PotentiallyUnmappedKeywordEsField unmappedEsField) {
            super(ctx.index, ctx.releasable, ctx.ctx, ctx.aliasFilter);
            this.unmappedEsField = unmappedEsField;
        }

        @Override
        public @Nullable MappedFieldType fieldType(String name) {
            var superResult = super.fieldType(name);
            return superResult == null && name.equals(unmappedEsField.getName()) ? createUnmappedFieldType(name, this) : superResult;
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

    private static @Nullable MultiTypeEsField findUnionTypes(Attribute attr) {
        if (attr instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField multiTypeEsField) {
            return multiTypeEsField;
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

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        final LuceneOperator.Factory luceneFactory;
        logger.trace("Query Exec is {}", esQueryExec);

        List<Sort> sorts = esQueryExec.sorts();
        assert esQueryExec.estimatedRowSize() != null : "estimated row size not initialized";
        int rowEstimatedSize = esQueryExec.estimatedRowSize();
        int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold(context.foldCtx()) : NO_LIMIT;
        boolean scoring = esQueryExec.hasScoring();
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
                topNAutoStrategy(),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit,
                sortBuilders,
                estimatedPerRowSortSize,
                scoring
            );
        } else if (esQueryExec.indexMode() == IndexMode.TIME_SERIES) {
            luceneFactory = new TimeSeriesSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.queryBuilderAndTags()),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit
            );
        } else {
            luceneFactory = new LuceneSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.queryBuilderAndTags()),
                context.queryPragmas().dataPartitioning(plannerSettings.defaultDataPartitioning()),
                context.autoPartitioningStrategy(),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(esQueryExec, rowEstimatedSize),
                limit,
                scoring
            );
        }
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    private static DataPartitioning.AutoStrategy topNAutoStrategy() {
        return unusedLimit -> {
            if (EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled()) {
                // Use high speed strategy for TopN - we want to parallelize searches as much as possible given the query structure
                return LuceneSourceOperator::highSpeedAutoStrategy;
            }
            return query -> LuceneSliceQueue.PartitioningStrategy.SHARD;
        };
    }

    List<ValuesSourceReaderOperator.FieldInfo> extractFields(FieldExtractExec fieldExtractExec) {
        List<Attribute> attributes = fieldExtractExec.attributesToExtract();
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(attributes.size());
        Set<String> nullsFilteredFields = new HashSet<>();
        fieldExtractExec.forEachDown(EsQueryExec.class, queryExec -> {
            QueryBuilder q = queryExec.queryBuilderAndTags().get(0).query();
            if (q != null) {
                nullsFilteredFields.addAll(nullsFilteredFieldsAfterSourceQuery(q));
            }
        });
        for (Attribute attr : attributes) {
            DataType dataType = attr.dataType();
            var fieldExtractPreference = fieldExtractExec.fieldExtractPreference(attr);
            ElementType elementType = PlannerUtils.toElementType(dataType, fieldExtractPreference);
            IntFunction<ValuesSourceReaderOperator.LoaderAndConverter> loaderAndConverter = s -> blockLoaderAndConverter(
                s,
                attr,
                fieldExtractPreference
            );
            String fieldName = getFieldName(attr);
            boolean nullsFiltered = nullsFilteredFields.contains(fieldName);
            fieldInfos.add(new ValuesSourceReaderOperator.FieldInfo(fieldName, elementType, nullsFiltered, loaderAndConverter));
        }
        return fieldInfos;
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
            context.queryPragmas().taskConcurrency(),
            tagTypes,
            limit == null ? NO_LIMIT : (Integer) limit.fold(context.foldCtx())
        );
    }

    @Override
    public Operator.OperatorFactory timeSeriesAggregatorOperatorFactory(
        TimeSeriesAggregateExec ts,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        List<BlockHash.GroupSpec> groupSpecs,
        LocalExecutionPlannerContext context
    ) {
        return new TimeSeriesAggregationOperator.Factory(
            ts.timeBucketRounding(context.foldCtx()),
            ts.timeBucket() != null && ts.timeBucket().dataType() == DataType.DATE_NANOS,
            groupSpecs,
            aggregatorMode,
            aggregatorFactories,
            context.pageSize(ts, ts.estimatedRowSize())
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
            var filter = sourcePaths != null ? new SourceFilter(sourcePaths.toArray(new String[0]), null) : null;
            // Apply vector exclusion logic similar to ShardGetService
            var fetchSourceContext = filter != null ? FetchSourceContext.of(true, null, filter.getIncludes(), filter.getExcludes()) : null;
            var result = maybeExcludeVectorFields(ctx.getMappingLookup(), ctx.getIndexSettings(), fetchSourceContext, null);
            var vectorFilter = result.v2();
            if (vectorFilter != null) {
                filter = vectorFilter;
            }
            return ctx.newSourceLoader(filter, false);
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
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference,
            BlockLoaderFunctionConfig blockLoaderFunctionConfig
        ) {
            if (asUnsupportedSource) {
                return ConstantNull.INSTANCE;
            }
            MappedFieldType fieldType = fieldType(name);
            if (fieldType == null) {
                // the field does not exist in this context
                return ConstantNull.INSTANCE;
            }
            BlockLoader loader = fieldType.blockLoader(new MappedFieldType.BlockLoaderContext() {
                @Override
                public String indexName() {
                    return ctx.getFullyQualifiedIndex().getName();
                }

                @Override
                public IndexSettings indexSettings() {
                    return ctx.getIndexSettings();
                }

                @Override
                public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                    return fieldExtractPreference;
                }

                @Override
                public SearchLookup lookup() {
                    return ctx.lookup();
                }

                @Override
                public Set<String> sourcePaths(String name) {
                    return ctx.sourcePath(name);
                }

                @Override
                public String parentField(String field) {
                    return ctx.parentPath(field);
                }

                @Override
                public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                    return (FieldNamesFieldMapper.FieldNamesFieldType) ctx.lookup().fieldType(FieldNamesFieldMapper.NAME);
                }

                @Override
                public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
                    return blockLoaderFunctionConfig;
                }

                @Override
                public MappingLookup mappingLookup() {
                    return ctx.getMappingLookup();
                }
            });
            if (loader == null) {
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
        public void close() {
            releasable.close();
        }
    }
}
