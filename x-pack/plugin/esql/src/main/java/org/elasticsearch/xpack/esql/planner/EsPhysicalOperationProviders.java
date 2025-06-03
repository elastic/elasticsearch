/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneCountOperator;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.TimeSeriesExtractFieldOperator;
import org.elasticsearch.compute.lucene.TimeSeriesSourceOperatorFactory;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.Sort;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesFieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter;
import static org.elasticsearch.compute.lucene.LuceneSourceOperator.NO_LIMIT;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;

public class EsPhysicalOperationProviders extends AbstractPhysicalOperationProviders {
    private static final Logger logger = LogManager.getLogger(EsPhysicalOperationProviders.class);

    /**
     * Context of each shard we're operating against.
     */
    public interface ShardContext extends org.elasticsearch.compute.lucene.ShardContext {

        /**
         * Convert a {@link QueryBuilder} into a real {@link Query lucene query}.
         */
        Query toQuery(QueryBuilder queryBuilder);

        /**
         * Tuning parameter for deciding when to use the "merge" stored field loader.
         * Think of it as "how similar to a sequential block of documents do I have to
         * be before I'll use the merge reader?" So a value of {@code 1} means I have to
         * be <strong>exactly</strong> a sequential block, like {@code 0, 1, 2, 3, .. 1299, 1300}.
         * A value of {@code .2} means we'll use the sequential reader even if we only
         * need one in ten documents.
         */
        double storedFieldsSequentialProportion();
    }

    private final List<ShardContext> shardContexts;
    private final DataPartitioning defaultDataPartitioning;

    public EsPhysicalOperationProviders(
        FoldContext foldContext,
        List<ShardContext> shardContexts,
        AnalysisRegistry analysisRegistry,
        DataPartitioning defaultDataPartitioning
    ) {
        super(foldContext, analysisRegistry);
        this.shardContexts = shardContexts;
        this.defaultDataPartitioning = defaultDataPartitioning;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        var sourceAttr = fieldExtractExec.sourceAttribute();
        List<ValuesSourceReaderOperator.ShardContext> readers = shardContexts.stream()
            .map(
                s -> new ValuesSourceReaderOperator.ShardContext(
                    s.searcher().getIndexReader(),
                    s::newSourceLoader,
                    s.storedFieldsSequentialProportion()
                )
            )
            .toList();
        int docChannel = source.layout.get(sourceAttr.id()).channel();
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.append(attr);
        }
        var fields = extractFields(fieldExtractExec.attributesToExtract(), fieldExtractExec::fieldExtractPreference);
        if (fieldExtractExec instanceof TimeSeriesFieldExtractExec) {
            // TODO: consolidate with ValuesSourceReaderOperator
            return source.with(new TimeSeriesExtractFieldOperator.Factory(fields, shardContexts), layout.build());
        } else {
            return source.with(new ValuesSourceReaderOperator.Factory(fields, readers, docChannel), layout.build());
        }
    }

    private static String getFieldName(Attribute attr) {
        // Do not use the field attribute name, this can deviate from the field name for union types.
        return attr instanceof FieldAttribute fa ? fa.fieldName() : attr.name();
    }

    private BlockLoader getBlockLoaderFor(int shardId, Attribute attr, MappedFieldType.FieldExtractPreference fieldExtractPreference) {
        DefaultShardContext shardContext = (DefaultShardContext) shardContexts.get(shardId);
        if (attr instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField kf) {
            shardContext = new DefaultShardContextForUnmappedField(shardContext, kf);
        }

        boolean isUnsupported = attr.dataType() == DataType.UNSUPPORTED;
        BlockLoader blockLoader = shardContext.blockLoader(getFieldName(attr), isUnsupported, fieldExtractPreference);
        MultiTypeEsField unionTypes = findUnionTypes(attr);
        if (unionTypes != null) {
            // Use the fully qualified name `cluster:index-name` because multiple types are resolved on coordinator with the cluster prefix
            String indexName = shardContext.ctx.getFullyQualifiedIndex().getName();
            Expression conversion = unionTypes.getConversionExpressionForIndex(indexName);
            return conversion == null
                ? BlockLoader.CONSTANT_NULLS
                : new TypeConvertingBlockLoader(blockLoader, (AbstractConvertFunction) conversion);
        }
        return blockLoader;
    }

    /** A hack to pretend an unmapped field still exists. */
    private static class DefaultShardContextForUnmappedField extends DefaultShardContext {
        private final KeywordEsField unmappedEsField;

        DefaultShardContextForUnmappedField(DefaultShardContext ctx, PotentiallyUnmappedKeywordEsField unmappedEsField) {
            super(ctx.index, ctx.ctx, ctx.aliasFilter);
            this.unmappedEsField = unmappedEsField;
        }

        @Override
        public @Nullable MappedFieldType fieldType(String name) {
            var superResult = super.fieldType(name);
            return superResult == null && name.equals(unmappedEsField.getName())
                ? new KeywordFieldMapper.KeywordFieldType(name, false /* isIndexed */, false /* hasDocValues */, Map.of() /* meta */)
                : superResult;
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

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        if (esQueryExec.indexMode() == IndexMode.TIME_SERIES) {
            assert false : "Time series source should be translated to TimeSeriesSourceExec";
            throw new IllegalStateException("Time series source should be translated to TimeSeriesSourceExec");
        }
        final LuceneOperator.Factory luceneFactory;
        logger.trace("Query Exec is {}", esQueryExec);

        List<Sort> sorts = esQueryExec.sorts();
        assert esQueryExec.estimatedRowSize() != null : "estimated row size not initialized";
        int rowEstimatedSize = esQueryExec.estimatedRowSize();
        int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold(context.foldCtx()) : NO_LIMIT;
        boolean scoring = esQueryExec.hasScoring();
        if ((sorts != null && sorts.isEmpty() == false)) {
            List<SortBuilder<?>> sortBuilders = new ArrayList<>(sorts.size());
            for (Sort sort : sorts) {
                sortBuilders.add(sort.sortBuilder());
            }
            luceneFactory = new LuceneTopNSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.query()),
                context.queryPragmas().dataPartitioning(defaultDataPartitioning),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(rowEstimatedSize),
                limit,
                sortBuilders,
                scoring
            );
        } else {
            luceneFactory = new LuceneSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.query()),
                context.queryPragmas().dataPartitioning(defaultDataPartitioning),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(rowEstimatedSize),
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

    @Override
    public PhysicalOperation timeSeriesSourceOperation(TimeSeriesSourceExec ts, LocalExecutionPlannerContext context) {
        final int limit = ts.limit() != null ? (Integer) ts.limit().fold(context.foldCtx()) : NO_LIMIT;
        LuceneOperator.Factory luceneFactory = TimeSeriesSourceOperatorFactory.create(
            limit,
            context.pageSize(ts.estimatedRowSize()),
            context.queryPragmas().taskConcurrency(),
            shardContexts,
            querySupplier(ts.query())
        );
        Layout.Builder layout = new Layout.Builder();
        layout.append(ts.output());
        context.driverParallelism(DriverParallelism.SINGLE);
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    private List<ValuesSourceReaderOperator.FieldInfo> extractFields(
        List<Attribute> attributes,
        Function<Attribute, MappedFieldType.FieldExtractPreference> preference
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            DataType dataType = attr.dataType();
            var fieldExtractPreference = preference.apply(attr);
            ElementType elementType = PlannerUtils.toElementType(dataType, fieldExtractPreference);
            IntFunction<BlockLoader> loader = s -> getBlockLoaderFor(s, attr, fieldExtractPreference);
            fieldInfos.add(new ValuesSourceReaderOperator.FieldInfo(getFieldName(attr), elementType, loader));
        }
        return fieldInfos;
    }

    /**
     * Build a {@link SourceOperator.SourceOperatorFactory} that counts documents in the search index.
     */
    public LuceneCountOperator.Factory countSource(LocalExecutionPlannerContext context, QueryBuilder queryBuilder, Expression limit) {
        return new LuceneCountOperator.Factory(
            shardContexts,
            querySupplier(queryBuilder),
            context.queryPragmas().dataPartitioning(defaultDataPartitioning),
            context.queryPragmas().taskConcurrency(),
            limit == null ? NO_LIMIT : (Integer) limit.fold(context.foldCtx())
        );
    }

    @Override
    public final Operator.OperatorFactory ordinalGroupingOperatorFactory(
        LocalExecutionPlanner.PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        LocalExecutionPlannerContext context
    ) {
        var sourceAttribute = FieldExtractExec.extractSourceAttributesFrom(aggregateExec.child());
        int docChannel = source.layout.get(sourceAttribute.id()).channel();
        List<ValuesSourceReaderOperator.ShardContext> vsShardContexts = shardContexts.stream()
            .map(
                s -> new ValuesSourceReaderOperator.ShardContext(
                    s.searcher().getIndexReader(),
                    s::newSourceLoader,
                    s.storedFieldsSequentialProportion()
                )
            )
            .toList();
        // The grouping-by values are ready, let's group on them directly.
        // Costin: why are they ready and not already exposed in the layout?
        return new OrdinalsGroupingOperator.OrdinalsGroupingOperatorFactory(
            shardIdx -> getBlockLoaderFor(shardIdx, attrSource, NONE),
            vsShardContexts,
            groupElementType,
            docChannel,
            attrSource.name(),
            aggregatorFactories,
            context.pageSize(aggregateExec.estimatedRowSize())
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
            shardContexts.size() == 1 && ts.anyMatch(p -> p instanceof TimeSeriesSourceExec),
            groupSpecs,
            aggregatorMode,
            aggregatorFactories,
            context.pageSize(ts.estimatedRowSize())
        );
    }

    public static class DefaultShardContext implements ShardContext {
        private final int index;
        private final SearchExecutionContext ctx;
        private final AliasFilter aliasFilter;
        private final String shardIdentifier;

        public DefaultShardContext(int index, SearchExecutionContext ctx, AliasFilter aliasFilter) {
            this.index = index;
            this.ctx = ctx;
            this.aliasFilter = aliasFilter;
            // Build the shardIdentifier once up front so we can reuse references to it in many places.
            this.shardIdentifier = ctx.getFullyQualifiedIndex().getName() + ":" + ctx.getShardId();
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
        public SourceLoader newSourceLoader() {
            return ctx.newSourceLoader(null, false);
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
            MappedFieldType.FieldExtractPreference fieldExtractPreference
        ) {
            if (asUnsupportedSource) {
                return BlockLoader.CONSTANT_NULLS;
            }
            MappedFieldType fieldType = fieldType(name);
            if (fieldType == null) {
                // the field does not exist in this context
                return BlockLoader.CONSTANT_NULLS;
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
            });
            if (loader == null) {
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", name);
                return BlockLoader.CONSTANT_NULLS;
            }

            return loader;
        }

        @Override
        public @Nullable MappedFieldType fieldType(String name) {
            return ctx.getFieldType(name);
        }

        @Override
        public double storedFieldsSequentialProportion() {
            return EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.get(ctx.getIndexSettings().getSettings());
        }
    }

    private static class TypeConvertingBlockLoader implements BlockLoader {
        private final BlockLoader delegate;
        private final TypeConverter typeConverter;

        protected TypeConvertingBlockLoader(BlockLoader delegate, AbstractConvertFunction convertFunction) {
            this.delegate = delegate;
            this.typeConverter = TypeConverter.fromConvertFunction(convertFunction);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            // Return the delegates builder, which can build the original mapped type, before conversion
            return delegate.builder(factory, expectedCount);
        }

        @Override
        public Block convert(Block block) {
            return typeConverter.convert((org.elasticsearch.compute.data.Block) block);
        }

        @Override
        public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            ColumnAtATimeReader reader = delegate.columnAtATimeReader(context);
            if (reader == null) {
                return null;
            }
            return new ColumnAtATimeReader() {
                @Override
                public Block read(BlockFactory factory, Docs docs) throws IOException {
                    Block block = reader.read(factory, docs);
                    return typeConverter.convert((org.elasticsearch.compute.data.Block) block);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return reader.toString();
                }
            };
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            // We do no type conversion here, since that will be done in the ValueSourceReaderOperator for row-stride cases
            // Using the BlockLoader.convert(Block) function defined above
            return delegate.rowStrideReader(context);
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return delegate.rowStrideStoredFieldSpec();
        }

        @Override
        public boolean supportsOrdinals() {
            // Fields with mismatching types cannot use ordinals for uniqueness determination, but must convert the values first
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new IllegalArgumentException("Ordinals are not supported for type conversion");
        }

        @Override
        public final String toString() {
            return "TypeConvertingBlockLoader[delegate=" + delegate + ", typeConverter=" + typeConverter + "]";
        }
    }
}
