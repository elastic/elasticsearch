/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Defines a kNN search to run in the search request.
 */
public class KnnSearchBuilder implements Writeable, ToXContentFragment, Rewriteable<KnnSearchBuilder> {
    public static final int NUM_CANDS_LIMIT = 10_000;
    public static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = 1.5f;

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField NAME_FIELD = AbstractQueryBuilder.NAME_FIELD;
    public static final ParseField BOOST_FIELD = AbstractQueryBuilder.BOOST_FIELD;
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final ParseField RESCORE_VECTOR_FIELD = new ParseField("rescore_vector");
    public static final ParseField OPTIMIZED_RESCORING_FIELD = new ParseField("optimized_rescoring");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnSearchBuilder.Builder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        // TODO optimize parsing for when BYTE values are provided
        return new Builder().field((String) args[0])
            .queryVector((VectorData) args[1])
            .queryVectorBuilder((QueryVectorBuilder) args[5])
            .k((Integer) args[2])
            .numCandidates((Integer) args[3])
            .visitPercentage((Float) args[4])
            .similarity((Float) args[6])
            .rescoreVectorBuilder((RescoreVectorBuilder) args[7]);
    });

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
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> RescoreVectorBuilder.fromXContent(p),
            RESCORE_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareBoolean(KnnSearchBuilder.Builder::optimizedRescoring, OPTIMIZED_RESCORING_FIELD);
        PARSER.declareFieldArray(
            KnnSearchBuilder.Builder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareString(KnnSearchBuilder.Builder::queryName, NAME_FIELD);
        PARSER.declareFloat(KnnSearchBuilder.Builder::boost, BOOST_FIELD);
        PARSER.declareField(
            KnnSearchBuilder.Builder::innerHit,
            (p, c) -> InnerHitBuilder.fromXContent(p),
            INNER_HITS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
    }

    public static KnnSearchBuilder.Builder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static final TransportVersion VISIT_PERCENTAGE = TransportVersion.fromName("visit_percentage");
    public static final TransportVersion KNN_DFS_RESCORING_TOP_K_ON_SHARDS = TransportVersion.fromName("knn_dfs_rescoring_top_k_on_shards");

    final String field;
    final VectorData queryVector;
    final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> querySupplier;
    final int k;
    final int numCands;
    final Float visitPercentage;
    final Float similarity;
    final List<QueryBuilder> filterQueries;
    String queryName;
    float boost = DEFAULT_BOOST;
    InnerHitBuilder innerHitBuilder;
    private final RescoreVectorBuilder rescoreVectorBuilder;
    private boolean optimizedRescoring;

    private static final RescoreVectorBuilder NO_RESCORING = new RescoreVectorBuilder(0);

    /**
     * Defines a kNN search.
     *
     * @param field                the name of the vector field to search against
     * @param queryVector          the query vector
     * @param k                    the final number of nearest neighbors to return as top hits
     * @param numCands             the number of nearest neighbor candidates to consider per shard
     * @param visitPercentage      percentage of the total number of vectors to visit per shard
     * @param rescoreVectorBuilder rescore vector information
     */
    public KnnSearchBuilder(
        String field,
        float[] queryVector,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(
            field,
            Objects.requireNonNull(VectorData.fromFloats(queryVector), format("[%s] cannot be null", QUERY_VECTOR_FIELD)),
            null,
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
            similarity
        );
    }

    /**
     * Defines a kNN search.
     *
     * @param field           the name of the vector field to search against
     * @param queryVector     the query vector
     * @param k               the final number of nearest neighbors to return as top hits
     * @param numCands        the number of nearest neighbor candidates to consider per shard
     * @param visitPercentage percentage of the total number of vectors to visit per shard
     */
    public KnnSearchBuilder(
        String field,
        VectorData queryVector,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(field, queryVector, null, k, numCands, visitPercentage, rescoreVectorBuilder, similarity);
    }

    /**
     * Defines a kNN search where the query vector will be provided by the queryVectorBuilder
     *
     * @param field              the name of the vector field to search against
     * @param queryVectorBuilder the query vector builder
     * @param k                  the final number of nearest neighbors to return as top hits
     * @param numCands           the number of nearest neighbor candidates to consider per shard
     * @param visitPercentage    percentage of the total number of vectors to visit per shard
     */
    public KnnSearchBuilder(
        String field,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(
            field,
            null,
            Objects.requireNonNull(queryVectorBuilder, format("[%s] cannot be null", QUERY_VECTOR_BUILDER_FIELD.getPreferredName())),
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
            similarity
        );
    }

    public KnnSearchBuilder(
        String field,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(
            field,
            queryVectorBuilder,
            queryVector,
            new ArrayList<>(),
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
            similarity,
            null,
            null,
            DEFAULT_BOOST,
            false
        );
    }

    private KnnSearchBuilder(
        String field,
        Supplier<float[]> querySupplier,
        Integer k,
        Integer numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        List<QueryBuilder> filterQueries,
        Float similarity
    ) {
        this.field = field;
        this.queryVector = VectorData.fromFloats(new float[0]);
        this.queryVectorBuilder = null;
        this.k = k;
        this.numCands = numCands;
        this.visitPercentage = visitPercentage;
        this.filterQueries = filterQueries;
        this.querySupplier = querySupplier;
        this.similarity = similarity;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
    }

    private KnnSearchBuilder(
        String field,
        QueryVectorBuilder queryVectorBuilder,
        VectorData queryVector,
        List<QueryBuilder> filterQueries,
        int k,
        int numCandidates,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity,
        InnerHitBuilder innerHitBuilder,
        String queryName,
        float boost,
        boolean optimizedRescoring
    ) {
        if (k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCandidates < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than " + "[" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (numCandidates > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        if (visitPercentage != null && (visitPercentage < 0.0f || visitPercentage > 100.0f)) {
            throw new IllegalArgumentException("[" + VISIT_PERCENTAGE_FIELD.getPreferredName() + "] must be between 0 and 100");
        }
        if (queryVector == null && queryVectorBuilder == null) {
            throw new IllegalArgumentException(
                format(
                    "either [%s] or [%s] must be provided",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    QUERY_VECTOR_FIELD.getPreferredName()
                )
            );
        }
        if (queryVector != null && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "cannot provide both [%s] and [%s]",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    QUERY_VECTOR_FIELD.getPreferredName()
                )
            );
        }
        this.field = field;
        this.queryVector = queryVector == null ? VectorData.fromFloats(new float[0]) : queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.k = k;
        this.numCands = numCandidates;
        this.visitPercentage = visitPercentage;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
        this.innerHitBuilder = innerHitBuilder;
        this.similarity = similarity;
        this.queryName = queryName;
        this.boost = boost;
        this.filterQueries = filterQueries;
        this.querySupplier = null;
        this.optimizedRescoring = optimizedRescoring;
    }

    public KnnSearchBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        if (in.getTransportVersion().supports(VISIT_PERCENTAGE)) {
            this.visitPercentage = in.readOptionalFloat();
        } else {
            this.visitPercentage = null;
        }
        this.queryVector = in.readOptionalWriteable(VectorData::new);
        this.filterQueries = in.readNamedWriteableCollectionAsList(QueryBuilder.class);
        this.boost = in.readFloat();
        this.queryName = in.readOptionalString();
        this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        this.querySupplier = null;
        this.similarity = in.readOptionalFloat();
        this.innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        this.rescoreVectorBuilder = in.readOptional(RescoreVectorBuilder::new);
        if (in.getTransportVersion().supports(KNN_DFS_RESCORING_TOP_K_ON_SHARDS)) {
            this.optimizedRescoring = in.readBoolean();
        }
    }

    public int k() {
        return k;
    }

    public int getNumCands() {
        return numCands;
    }

    public Float getVisitPercentage() {
        return visitPercentage;
    }

    public RescoreVectorBuilder getRescoreVectorBuilder() {
        return rescoreVectorBuilder;
    }

    public QueryVectorBuilder getQueryVectorBuilder() {
        return queryVectorBuilder;
    }

    // for testing only
    public VectorData getQueryVector() {
        return queryVector;
    }

    public String getField() {
        return field;
    }

    public List<QueryBuilder> getFilterQueries() {
        return filterQueries;
    }

    public KnnSearchBuilder addFilterQuery(QueryBuilder filterQuery) {
        Objects.requireNonNull(filterQuery);
        this.filterQueries.add(filterQuery);
        return this;
    }

    public KnnSearchBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filterQueries.addAll(filterQueries);
        return this;
    }

    /**
     * Sets a query name for the kNN search query.
     */
    public KnnSearchBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    public String queryName() {
        return queryName;
    }

    /**
     * Set a boost to apply to the kNN search scores.
     */
    public KnnSearchBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public float boost() {
        return boost;
    }

    public KnnSearchBuilder innerHit(InnerHitBuilder innerHitBuilder) {
        this.innerHitBuilder = innerHitBuilder;
        return this;
    }

    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    @Override
    public KnnSearchBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (querySupplier != null) {
            if (querySupplier.get() == null) {
                return this;
            }
            return new KnnSearchBuilder(field, querySupplier.get(), k, numCands, visitPercentage, rescoreVectorBuilder, similarity).boost(
                boost
            ).queryName(queryName).addFilterQueries(filterQueries).innerHit(innerHitBuilder).optimizedRescoring(optimizedRescoring);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
                toSet.set(v);
                if (v == null) {
                    ll.onFailure(
                        new IllegalArgumentException(
                            format(
                                "[%s] with name [%s] returned null query_vector",
                                QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                                queryVectorBuilder.getWriteableName()
                            )
                        )
                    );
                    return;
                }
                ll.onResponse(null);
            })));
            return new KnnSearchBuilder(field, toSet::get, k, numCands, visitPercentage, rescoreVectorBuilder, filterQueries, similarity)
                .boost(boost)
                .queryName(queryName)
                .innerHit(innerHitBuilder)
                .optimizedRescoring(optimizedRescoring);
        }
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filterQueries.size());
        for (QueryBuilder query : filterQueries) {
            QueryBuilder rewrittenQuery = query.rewrite(ctx);
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return new KnnSearchBuilder(field, queryVector, k, numCands, visitPercentage, rescoreVectorBuilder, similarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(rewrittenQueries)
                .innerHit(innerHitBuilder)
                .optimizedRescoring(optimizedRescoring);
        }
        return this;
    }

    private KnnSearchBuilder optimizedRescoring(boolean optimizedRescoring) {
        this.optimizedRescoring = optimizedRescoring;
        return this;
    }

    public KnnVectorQueryBuilder toQueryBuilder(SearchExecutionContext searchExecutionContext) {
        if (queryVectorBuilder != null) {
            throw new IllegalArgumentException("missing rewrite");
        }
        if (optimizedRescoring) {
            Float oversample = getOversampleFactor(searchExecutionContext);
            int localK = oversample <= 1 ? k : (int) Math.ceil(k * oversample);
            int localNumcands = oversample == 0 ? numCands : Math.max(localK, numCands);
            return new KnnVectorQueryBuilder(field, queryVector, localK, localNumcands, visitPercentage, NO_RESCORING, similarity).boost(
                boost
            ).queryName(queryName).addFilterQueries(filterQueries);
        } else {
            return new KnnVectorQueryBuilder(field, queryVector, k, numCands, visitPercentage, rescoreVectorBuilder, similarity).boost(
                boost
            ).queryName(queryName).addFilterQueries(filterQueries);
        }
    }

    public Float getOversampleFactor(SearchExecutionContext searchExecutionContext) {
        if (false == optimizedRescoring) {
            return null;
        }
        return rescoreVectorBuilder != null
            ? rescoreVectorBuilder.oversample()
            : getDefaultOversampleForField(field, searchExecutionContext);
    }

    private static float getDefaultOversampleForField(String fieldName, SearchExecutionContext searchExecutionContext) {
        assert searchExecutionContext != null : "[searchExecutionContext] should be available";
        var fieldType = searchExecutionContext.getFieldType(fieldName);
        var indexOptions = fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType
            ? ((DenseVectorFieldMapper.DenseVectorFieldType) fieldType).getIndexOptions()
            : null;
        var quantizedIndexOptions = indexOptions instanceof DenseVectorFieldMapper.QuantizedIndexOptions
            ? ((DenseVectorFieldMapper.QuantizedIndexOptions) indexOptions).getRescoreVector()
            : null;
        return quantizedIndexOptions != null ? quantizedIndexOptions.oversample() : 0f;
    }

    public Float getSimilarity() {
        return similarity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnSearchBuilder that = (KnnSearchBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(visitPercentage, that.visitPercentage)
            && Objects.equals(rescoreVectorBuilder, that.rescoreVectorBuilder)
            && Objects.equals(field, that.field)
            && Objects.equals(queryVector, that.queryVector)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(querySupplier, that.querySupplier)
            && Objects.equals(filterQueries, that.filterQueries)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(queryName, that.queryName)
            && boost == that.boost
            && optimizedRescoring == that.optimizedRescoring;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            k,
            numCands,
            visitPercentage,
            querySupplier,
            queryVectorBuilder,
            rescoreVectorBuilder,
            similarity,
            Objects.hashCode(queryVector),
            Objects.hashCode(filterQueries),
            innerHitBuilder,
            queryName,
            boost,
            optimizedRescoring
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);

        if (visitPercentage != null) {
            builder.field(VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentage);
        }

        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        } else {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }
        if (similarity != null) {
            builder.field(VECTOR_SIMILARITY.getPreferredName(), similarity);
        }

        if (filterQueries.isEmpty() == false) {
            builder.startArray(FILTER_FIELD.getPreferredName());
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }

        if (boost != DEFAULT_BOOST) {
            builder.field(BOOST_FIELD.getPreferredName(), boost);
        }
        if (queryName != null) {
            builder.field(NAME_FIELD.getPreferredName(), queryName);
        }
        if (rescoreVectorBuilder != null) {
            builder.field(RESCORE_VECTOR_FIELD.getPreferredName(), rescoreVectorBuilder);
        }
        builder.field(OPTIMIZED_RESCORING_FIELD.getPreferredName(), optimizedRescoring);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (querySupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(field);
        out.writeVInt(k);
        out.writeVInt(numCands);
        if (out.getTransportVersion().supports(VISIT_PERCENTAGE)) {
            out.writeOptionalFloat(visitPercentage);
        }
        out.writeOptionalWriteable(queryVector);
        out.writeNamedWriteableCollection(filterQueries);
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
        out.writeOptionalNamedWriteable(queryVectorBuilder);
        out.writeOptionalFloat(similarity);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeOptionalWriteable(rescoreVectorBuilder);
        if (out.getTransportVersion().supports(KNN_DFS_RESCORING_TOP_K_ON_SHARDS)) {
            out.writeBoolean(optimizedRescoring);
        }
    }

    public static class Builder {

        private String field;
        private VectorData queryVector;
        private QueryVectorBuilder queryVectorBuilder;
        private Integer k;
        private Integer numCandidates;
        private Float visitPercentage;
        private Float similarity;
        private final List<QueryBuilder> filterQueries = new ArrayList<>();
        private String queryName;
        private float boost = DEFAULT_BOOST;
        private InnerHitBuilder innerHitBuilder;
        private RescoreVectorBuilder rescoreVectorBuilder;
        private boolean optimizedRescoring = false;

        public Builder addFilterQueries(List<QueryBuilder> filterQueries) {
            Objects.requireNonNull(filterQueries);
            this.filterQueries.addAll(filterQueries);
            return this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        public Builder queryName(String queryName) {
            this.queryName = queryName;
            return this;
        }

        public Builder boost(float boost) {
            this.boost = boost;
            return this;
        }

        public Builder innerHit(InnerHitBuilder innerHitBuilder) {
            this.innerHitBuilder = innerHitBuilder;
            return this;
        }

        public Builder queryVector(VectorData queryVector) {
            this.queryVector = queryVector;
            return this;
        }

        public Builder queryVectorBuilder(QueryVectorBuilder queryVectorBuilder) {
            this.queryVectorBuilder = queryVectorBuilder;
            return this;
        }

        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        public Builder numCandidates(Integer numCands) {
            this.numCandidates = numCands;
            return this;
        }

        public Builder visitPercentage(Float visitPercentage) {
            this.visitPercentage = visitPercentage;
            return this;
        }

        public Builder similarity(Float similarity) {
            this.similarity = similarity;
            return this;
        }

        public Builder rescoreVectorBuilder(RescoreVectorBuilder rescoreVectorBuilder) {
            this.rescoreVectorBuilder = rescoreVectorBuilder;
            return this;
        }

        public Builder optimizedRescoring(boolean optimizedRescoring) {
            this.optimizedRescoring = optimizedRescoring;
            return this;
        }

        public KnnSearchBuilder build(int size) {
            int requestSize = size < 0 ? DEFAULT_SIZE : size;
            int adjustedK = k == null ? requestSize : k;
            int adjustedNumCandidates = numCandidates == null
                ? Math.round(Math.min(NUM_CANDS_LIMIT, NUM_CANDS_MULTIPLICATIVE_FACTOR * adjustedK))
                : numCandidates;
            return new KnnSearchBuilder(
                field,
                queryVectorBuilder,
                queryVector,
                filterQueries,
                adjustedK,
                adjustedNumCandidates,
                visitPercentage,
                rescoreVectorBuilder,
                similarity,
                innerHitBuilder,
                queryName,
                boost,
                optimizedRescoring
            );
        }
    }
}
