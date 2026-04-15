/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.ToChildBlockJoinQueryBuilder;
import org.elasticsearch.index.query.support.AutoPrefilteringUtils;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that performs kNN search using Lucene's {@link org.apache.lucene.search.KnnFloatVectorQuery} or
 * {@link org.apache.lucene.search.KnnByteVectorQuery}.
 */
public class KnnVectorQueryBuilder extends LeafQueryBuilder<KnnVectorQueryBuilder> {

    private static final Logger logger = LogManager.getLogger(KnnVectorQueryBuilder.class);

    public static final TransportVersion AUTO_PREFILTERING = TransportVersion.fromName("knn_vector_query_auto_prefiltering");

    public static final String NAME = "knn";
    private static final int NUM_CANDS_LIMIT = 10_000;
    private static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = 1.5f;

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField VECTOR_SIMILARITY_FIELD = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField RESCORE_VECTOR_FIELD = new ParseField("rescore_vector");

    public static final ConstructingObjectParser<KnnVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "knn",
        args -> new KnnVectorQueryBuilder(
            (String) args[0],
            (VectorData) args[1],
            (QueryVectorBuilder) args[6],
            null,
            (Integer) args[2],
            (Integer) args[3],
            (Float) args[4],
            (RescoreVectorBuilder) args[7],
            (Float) args[5],
            null
        )
    );

    /**
     * Semantic knn queries will be wrapped in a nested query as the target field is the nested embeddings vector.
     * knn pre-filtering does not handle nested filters when the knn itself is in a nested query.
     * (see <a href="https://github.com/elastic/elasticsearch/issues/138410">elasticsearch/#138410</a>)
     */
    public static final Set<Class<? extends QueryBuilder>> UNSUPPORTED_AUTO_PREFILTERING_QUERY_TYPES = Set.of(NestedQueryBuilder.class);

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> VectorData.parseXContent(p),
            QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER
        );
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
        PARSER.declareInt(optionalConstructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VISIT_PERCENTAGE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> RescoreVectorBuilder.fromXContent(p),
            RESCORE_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareFieldArray(
            KnnVectorQueryBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        declareStandardFields(PARSER);
    }

    public static KnnVectorQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final TransportVersion VISIT_PERCENTAGE = TransportVersion.fromName("visit_percentage");

    private final String fieldName;
    private final VectorData queryVector;
    private final Integer k;
    private final Integer numCands;
    private final Float visitPercentage;
    private final List<QueryBuilder> filterQueries = new ArrayList<>();
    private final Float vectorSimilarity;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> queryVectorSupplier;
    private final RescoreVectorBuilder rescoreVectorBuilder;

    /**
     * Inference ID extracted from the query vector builder during rewrite, carried forward
     * to {@link #doToQuery} where it can be persisted to the field mapping.
     */
    private String inferenceIdForMapping;

    /**
     * True if auto pre-filtering should be applied.
     */
    private boolean isAutoPrefilteringEnabled = false;

    public KnnVectorQueryBuilder(
        String fieldName,
        float[] queryVector,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float vectorSimilarity
    ) {
        this(
            fieldName,
            VectorData.fromFloats(queryVector),
            null,
            null,
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
            vectorSimilarity,
            null
        );
    }

    public KnnVectorQueryBuilder(
        String fieldName,
        QueryVectorBuilder queryVectorBuilder,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        Float vectorSimilarity
    ) {
        this(fieldName, null, queryVectorBuilder, null, k, numCands, visitPercentage, null, vectorSimilarity, null);
    }

    public KnnVectorQueryBuilder(
        String fieldName,
        byte[] queryVector,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float vectorSimilarity
    ) {
        this(
            fieldName,
            VectorData.fromBytes(queryVector),
            null,
            null,
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
            vectorSimilarity,
            null
        );
    }

    public KnnVectorQueryBuilder(
        String fieldName,
        VectorData queryVector,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float vectorSimilarity
    ) {
        this(fieldName, queryVector, null, null, k, numCands, visitPercentage, rescoreVectorBuilder, vectorSimilarity, null);
    }

    private KnnVectorQueryBuilder(
        String fieldName,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        Supplier<float[]> queryVectorSupplier,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float vectorSimilarity,
        String inferenceIdForMapping
    ) {
        if (k != null && k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCands != null && numCands > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        if (k != null && numCands != null && numCands < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than [" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (visitPercentage != null && (visitPercentage < 0.0f || visitPercentage > 100.0f)) {
            throw new IllegalArgumentException("[" + VISIT_PERCENTAGE_FIELD.getPreferredName() + "] must be between 0.0 and 100.0");
        }
        if (queryVector == null && queryVectorBuilder == null) {
            throw new IllegalArgumentException(
                format(
                    "either [%s] or [%s] must be provided",
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        } else if (queryVector != null && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "only one of [%s] and [%s] must be provided",
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                )
            );
        }
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.k = k;
        this.numCands = numCands;
        this.visitPercentage = visitPercentage;
        this.vectorSimilarity = vectorSimilarity;
        this.queryVectorBuilder = queryVectorBuilder;
        this.queryVectorSupplier = queryVectorSupplier;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
        this.inferenceIdForMapping = inferenceIdForMapping;
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.k = in.readOptionalVInt();
        this.numCands = in.readOptionalVInt();
        if (in.getTransportVersion().supports(VISIT_PERCENTAGE)) {
            this.visitPercentage = in.readOptionalFloat();
        } else {
            this.visitPercentage = null;
        }
        this.queryVector = in.readOptionalWriteable(VectorData::new);
        this.filterQueries.addAll(readQueries(in));
        this.vectorSimilarity = in.readOptionalFloat();
        this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        this.rescoreVectorBuilder = in.readOptional(RescoreVectorBuilder::new);
        if (in.getTransportVersion().supports(AUTO_PREFILTERING)) {
            this.isAutoPrefilteringEnabled = in.readBoolean();
        }

        this.queryVectorSupplier = null;
        this.inferenceIdForMapping = null;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public VectorData queryVector() {
        return queryVector;
    }

    @Nullable
    public Float getVectorSimilarity() {
        return vectorSimilarity;
    }

    public Integer k() {
        return k;
    }

    public Integer numCands() {
        return numCands;
    }

    public Float visitPercentage() {
        return visitPercentage;
    }

    public List<QueryBuilder> filterQueries() {
        return filterQueries;
    }

    @Nullable
    public QueryVectorBuilder queryVectorBuilder() {
        return queryVectorBuilder;
    }

    public RescoreVectorBuilder rescoreVectorBuilder() {
        return rescoreVectorBuilder;
    }

    public KnnVectorQueryBuilder addFilterQuery(QueryBuilder filterQuery) {
        Objects.requireNonNull(filterQuery);
        this.filterQueries.add(filterQuery);
        return this;
    }

    public KnnVectorQueryBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filterQueries.addAll(filterQueries);
        return this;
    }

    public KnnVectorQueryBuilder setFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filterQueries.clear();
        this.filterQueries.addAll(filterQueries);
        return this;
    }

    public boolean isAutoPrefilteringEnabled() {
        return isAutoPrefilteringEnabled;
    }

    void setInferenceIdForMapping(String inferenceIdForMapping) {
        this.inferenceIdForMapping = inferenceIdForMapping;
    }

    String inferenceIdForMapping() {
        return inferenceIdForMapping;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalVInt(k);
        out.writeOptionalVInt(numCands);
        if (out.getTransportVersion().supports(VISIT_PERCENTAGE)) {
            out.writeOptionalFloat(visitPercentage);
        }
        out.writeOptionalWriteable(queryVector);
        writeQueries(out, filterQueries);
        out.writeOptionalFloat(vectorSimilarity);
        out.writeOptionalNamedWriteable(queryVectorBuilder);
        out.writeOptionalWriteable(rescoreVectorBuilder);
        if (out.getTransportVersion().supports(AUTO_PREFILTERING)) {
            out.writeBoolean(isAutoPrefilteringEnabled);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryVectorSupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }
        if (k != null) {
            builder.field(K_FIELD.getPreferredName(), k);
        }
        if (numCands != null) {
            builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        }
        if (visitPercentage != null) {
            builder.field(VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentage);
        }
        if (vectorSimilarity != null) {
            builder.field(VECTOR_SIMILARITY_FIELD.getPreferredName(), vectorSimilarity);
        }
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        }
        if (filterQueries.isEmpty() == false) {
            builder.startArray(FILTER_FIELD.getPreferredName());
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (rescoreVectorBuilder != null) {
            builder.field(RESCORE_VECTOR_FIELD.getPreferredName(), rescoreVectorBuilder);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext ctx) throws IOException {
        if (queryVectorSupplier != null) {
            if (queryVectorSupplier.get() == null) {
                return this;
            }
            String resolvedInferenceId = queryVectorBuilder != null ? queryVectorBuilder.getInferenceId() : null;
            return new KnnVectorQueryBuilder(
                fieldName,
                VectorData.fromFloats(queryVectorSupplier.get()),
                null,
                null,
                k,
                numCands,
                visitPercentage,
                rescoreVectorBuilder,
                vectorSimilarity,
                resolvedInferenceId
            ).boost(boost).queryName(queryName).addFilterQueries(filterQueries).setAutoPrefilteringEnabled(isAutoPrefilteringEnabled);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerUniqueAsyncAction(new QueryVectorBuilderAsyncAction(queryVectorBuilder), toSet::set);
            return new KnnVectorQueryBuilder(
                fieldName,
                queryVector,
                queryVectorBuilder,
                toSet::get,
                k,
                numCands,
                visitPercentage,
                rescoreVectorBuilder,
                vectorSimilarity,
                null
            ).boost(boost).queryName(queryName).addFilterQueries(filterQueries).setAutoPrefilteringEnabled(isAutoPrefilteringEnabled);
        }
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filterQueries.size());
        for (QueryBuilder query : filterQueries) {
            QueryBuilder rewrittenQuery = query.rewrite(ctx);
            if (rewrittenQuery instanceof MatchNoneQueryBuilder) {
                return rewrittenQuery;
            }
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return new KnnVectorQueryBuilder(
                fieldName,
                queryVector,
                queryVectorBuilder,
                queryVectorSupplier,
                k,
                numCands,
                visitPercentage,
                rescoreVectorBuilder,
                vectorSimilarity,
                inferenceIdForMapping
            ).boost(boost).queryName(queryName).addFilterQueries(rewrittenQueries).setAutoPrefilteringEnabled(isAutoPrefilteringEnabled);
        }
        if (ctx.convertToInnerHitsRewriteContext() != null) {
            QueryBuilder exactKnnQuery = new ExactKnnQueryBuilder(queryVector, fieldName, vectorSimilarity);
            if (filterQueries.isEmpty()) {
                return exactKnnQuery;
            } else {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                boolQuery.must(exactKnnQuery);
                for (QueryBuilder filter : this.filterQueries) {
                    // filter can be both over parents or nested docs, so add them as should clauses to a filter
                    BoolQueryBuilder adjustedFilter = new BoolQueryBuilder().should(filter)
                        .should(new ToChildBlockJoinQueryBuilder(filter));
                    boolQuery.filter(adjustedFilter);
                }
                return boolQuery;
            }
        }
        return this;
    }

    /**
     * If the target dense_vector field has no inference_id set, and the resolved inference ID is available,
     * and the vector's dimensions match, fires a best-effort mapping update to persist the inference_id.
     * Failures are logged and swallowed — the search is never affected.
     */
    private void maybeUpdateInferenceIdMapping(SearchExecutionContext context, DenseVectorFieldType vectorFieldType) {
        if (inferenceIdForMapping == null) {
            return;
        }
        if (vectorFieldType.getInferenceId() != null) {
            return;
        }
        // TODO: Re-enable index version gate before moving out of POC
        // if (context.getIndexSettings().getIndexVersionCreated().before(IndexVersions.DENSE_VECTOR_INFERENCE_ID)) {
        // return;
        // }
        if (queryVector != null && vectorFieldType.getDims() != null && vectorFieldType.getDims() != queryVector.asFloatVector().length) {
            return;
        }
        Index index = context.getFullyQualifiedIndex();
        try {
            PutMappingRequest request = new PutMappingRequest();
            request.setConcreteIndex(index);
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.startObject("properties");
            builder.startObject(fieldName);
            builder.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
            if (vectorFieldType.getDims() != null) {
                builder.field("dims", vectorFieldType.getDims());
            }
            builder.field("inference_id", inferenceIdForMapping);
            builder.endObject();
            builder.endObject();
            builder.endObject();
            request.source(builder);
            request.masterNodeTimeout(TimeValue.timeValueSeconds(30));
            request.ackTimeout(TimeValue.ZERO);
            context.getClient().execute(TransportAutoPutMappingAction.TYPE, request, ActionListener.wrap(response -> {
                logger.debug("Updated inference_id [{}] on dense_vector field [{}] in index [{}]", inferenceIdForMapping, fieldName, index);
            }, failure -> {
                logger.debug(
                    () -> format(
                        "Failed to update inference_id [%s] on dense_vector field [%s] in index [%s]",
                        inferenceIdForMapping,
                        fieldName,
                        index
                    ),
                    failure
                );
            }));
        } catch (Exception e) {
            logger.debug(
                () -> format(
                    "Failed to build mapping update for inference_id [%s] on field [%s] in index [%s]",
                    inferenceIdForMapping,
                    fieldName,
                    index
                ),
                e
            );
        }
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        int k;
        if (this.k != null) {
            k = this.k;
        } else {
            k = context.requestSize() == null || context.requestSize() < 0 ? DEFAULT_SIZE : context.requestSize();
            if (numCands != null) {
                k = Math.min(k, numCands);
            }
        }
        int adjustedNumCands = numCands == null ? Math.round(Math.min(NUM_CANDS_MULTIPLICATIVE_FACTOR * k, NUM_CANDS_LIMIT)) : numCands;

        if (fieldType == null) {
            return Queries.NO_DOCS_INSTANCE;
        }
        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }
        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;

        maybeUpdateInferenceIdMapping(context, vectorFieldType);

        List<Query> filtersInitial = doFiltersToQuery(context);

        String parentPath = context.nestedLookup().getNestedParent(fieldName);
        BitSetProducer parentBitSet = null;
        Query filterQuery;
        if (parentPath == null) {
            filterQuery = buildFilterQuery(filtersInitial);
        } else {
            final Query parentFilter;
            NestedObjectMapper originalObjectMapper = context.nestedScope().getObjectMapper();
            if (originalObjectMapper != null) {
                try {
                    // we are in a nested context, to get the parent filter we need to go up one level
                    context.nestedScope().previousLevel();
                    NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
                    parentFilter = objectMapper == null
                        ? Queries.newNonNestedFilter(context.indexVersionCreated())
                        : objectMapper.nestedTypeFilter();
                } finally {
                    context.nestedScope().nextLevel(originalObjectMapper);
                }
            } else {
                // we are NOT in a nested context, coming from the top level knn search
                parentFilter = Queries.newNonNestedFilter(context.indexVersionCreated());
            }
            parentBitSet = context.bitsetFilter(parentFilter);
            List<Query> filterAdjusted = new ArrayList<>(filtersInitial.size());
            for (Query f : filtersInitial) {
                // If filter matches non-nested docs, we assume this is a filter over parents docs,
                // so we will modify it accordingly: matching parents docs with join to its child docs
                if (NestedHelper.mightMatchNonNestedDocs(f, parentPath, context)) {
                    // Ensure that the query only returns parent documents matching filter
                    f = Queries.filtered(f, parentFilter);
                    f = new ToChildBlockJoinQuery(f, parentBitSet);
                }
                filterAdjusted.add(f);
            }
            filterQuery = buildFilterQuery(filterAdjusted);
        }

        DenseVectorFieldMapper.FilterHeuristic heuristic = context.getIndexSettings().getHnswFilterHeuristic();
        boolean hnswEarlyTermination = context.getIndexSettings().getHnswEarlyTermination();
        Float oversample = rescoreVectorBuilder() == null ? null : rescoreVectorBuilder.oversample();
        if (filterQuery != null && (vectorFieldType.getIndexOptions() == null || vectorFieldType.getIndexOptions().isFlat() == false)) {
            // Force the filter to be cacheable because it will be eagerly transformed into a bitset.
            // Simple filters (e.g., term queries) are normally considered too cheap to cache by the
            // default strategy, but once materialized as a bitset on every execution they become
            // significantly more expensive, making caching essential.
            filterQuery = new CachingEnableFilterQuery(filterQuery);
        }

        return vectorFieldType.createKnnQuery(
            queryVector,
            k,
            adjustedNumCands,
            visitPercentage,
            oversample,
            filterQuery,
            vectorSimilarity,
            parentBitSet,
            heuristic,
            hnswEarlyTermination
        );
    }

    private List<Query> doFiltersToQuery(SearchExecutionContext context) throws IOException {
        final List<Query> autoPrefilters = applyAutoPrefilteringIfEnabled(context);
        final List<Query> allFilters = new ArrayList<>(
            filterQueries.size() + autoPrefilters.size() + (context.getAliasFilter() == null ? 0 : 1)
        );
        allFilters.addAll(autoPrefilters);
        for (QueryBuilder queryBuilder : filterQueries) {
            allFilters.add(queryBuilder.toQuery(context));
        }
        if (context.getAliasFilter() != null) {
            allFilters.add(context.getAliasFilter().toQuery(context));
        }
        return allFilters;
    }

    private List<Query> applyAutoPrefilteringIfEnabled(SearchExecutionContext context) throws IOException {
        if (isAutoPrefilteringEnabled == false) {
            return List.of();
        }
        final List<Query> autoPrefilters = new ArrayList<>();
        for (QueryBuilder queryBuilder : context.autoPrefilteringScope().getPrefilters().stream().filter(f -> this != f).toList()) {
            Optional<QueryBuilder> pruned = AutoPrefilteringUtils.pruneQuery(queryBuilder, UNSUPPORTED_AUTO_PREFILTERING_QUERY_TYPES);
            if (pruned.isPresent()) {
                Query query = pruned.get().toQuery(context);
                autoPrefilters.add(query);
            }
        }
        return autoPrefilters;
    }

    private static Query buildFilterQuery(List<Query> filters) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Query f : filters) {
            builder.add(f, BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        return filterQuery;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            fieldName,
            Objects.hashCode(queryVector),
            k,
            numCands,
            visitPercentage,
            filterQueries,
            vectorSimilarity,
            queryVectorBuilder,
            rescoreVectorBuilder,
            isAutoPrefilteringEnabled
        );
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(queryVector, other.queryVector)
            && Objects.equals(k, other.k)
            && Objects.equals(numCands, other.numCands)
            && Objects.equals(visitPercentage, other.visitPercentage)
            && Objects.equals(filterQueries, other.filterQueries)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity)
            && Objects.equals(queryVectorBuilder, other.queryVectorBuilder)
            && Objects.equals(rescoreVectorBuilder, other.rescoreVectorBuilder)
            && isAutoPrefilteringEnabled == other.isAutoPrefilteringEnabled;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    public KnnVectorQueryBuilder setAutoPrefilteringEnabled(boolean isAutoPrefilteringEnabled) {
        this.isAutoPrefilteringEnabled = isAutoPrefilteringEnabled;
        return this;
    }
}
