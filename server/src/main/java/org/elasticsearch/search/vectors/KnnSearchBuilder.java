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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
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

import static org.elasticsearch.TransportVersions.V_8_11_X;
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
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField NAME_FIELD = AbstractQueryBuilder.NAME_FIELD;
    public static final ParseField BOOST_FIELD = AbstractQueryBuilder.BOOST_FIELD;
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final ParseField RESCORE_VECTOR_FIELD = new ParseField("rescore_vector");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnSearchBuilder.Builder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        // TODO optimize parsing for when BYTE values are provided
        return new Builder().field((String) args[0])
            .queryVector((VectorData) args[1])
            .queryVectorBuilder((QueryVectorBuilder) args[4])
            .k((Integer) args[2])
            .numCandidates((Integer) args[3])
            .similarity((Float) args[5])
            .rescoreVectorBuilder((RescoreVectorBuilder) args[6]);
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

    final String field;
    final VectorData queryVector;
    final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> querySupplier;
    final int k;
    final int numCands;
    final Float similarity;
    final List<QueryBuilder> filterQueries;
    String queryName;
    float boost = DEFAULT_BOOST;
    InnerHitBuilder innerHitBuilder;
    private final RescoreVectorBuilder rescoreVectorBuilder;

    /**
     * Defines a kNN search.
     *
     * @param field       the name of the vector field to search against
     * @param queryVector the query vector
     * @param k           the final number of nearest neighbors to return as top hits
     * @param numCands    the number of nearest neighbor candidates to consider per shard
     * @param rescoreVectorBuilder rescore vector information
     */
    public KnnSearchBuilder(
        String field,
        float[] queryVector,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(
            field,
            Objects.requireNonNull(VectorData.fromFloats(queryVector), format("[%s] cannot be null", QUERY_VECTOR_FIELD)),
            null,
            k,
            numCands,
            rescoreVectorBuilder,
            similarity
        );
    }

    /**
     * Defines a kNN search.
     *
     * @param field       the name of the vector field to search against
     * @param queryVector the query vector
     * @param k           the final number of nearest neighbors to return as top hits
     * @param numCands    the number of nearest neighbor candidates to consider per shard
     */
    public KnnSearchBuilder(
        String field,
        VectorData queryVector,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(field, queryVector, null, k, numCands, rescoreVectorBuilder, similarity);
    }

    /**
     * Defines a kNN search where the query vector will be provided by the queryVectorBuilder
     *
     * @param field              the name of the vector field to search against
     * @param queryVectorBuilder the query vector builder
     * @param k                  the final number of nearest neighbors to return as top hits
     * @param numCands           the number of nearest neighbor candidates to consider per shard
     */
    public KnnSearchBuilder(
        String field,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(
            field,
            null,
            Objects.requireNonNull(queryVectorBuilder, format("[%s] cannot be null", QUERY_VECTOR_BUILDER_FIELD.getPreferredName())),
            k,
            numCands,
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
            rescoreVectorBuilder,
            similarity,
            null,
            null,
            DEFAULT_BOOST
        );
    }

    private KnnSearchBuilder(
        String field,
        Supplier<float[]> querySupplier,
        Integer k,
        Integer numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        List<QueryBuilder> filterQueries,
        Float similarity
    ) {
        this.field = field;
        this.queryVector = VectorData.fromFloats(new float[0]);
        this.queryVectorBuilder = null;
        this.k = k;
        this.numCands = numCands;
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
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity,
        InnerHitBuilder innerHitBuilder,
        String queryName,
        float boost
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
        this.rescoreVectorBuilder = rescoreVectorBuilder;
        this.innerHitBuilder = innerHitBuilder;
        this.similarity = similarity;
        this.queryName = queryName;
        this.boost = boost;
        this.filterQueries = filterQueries;
        this.querySupplier = null;
    }

    public KnnSearchBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.queryVector = in.readOptionalWriteable(VectorData::new);
        } else {
            this.queryVector = VectorData.fromFloats(in.readFloatArray());
        }
        this.filterQueries = in.readNamedWriteableCollectionAsList(QueryBuilder.class);
        this.boost = in.readFloat();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.queryName = in.readOptionalString();
        } else {
            this.queryName = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        } else {
            this.queryVectorBuilder = null;
        }
        this.querySupplier = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            this.similarity = in.readOptionalFloat();
        } else {
            this.similarity = null;
        }
        if (in.getTransportVersion().onOrAfter(V_8_11_X)) {
            this.innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.KNN_QUERY_RESCORE_OVERSAMPLE)) {
            this.rescoreVectorBuilder = in.readOptional(RescoreVectorBuilder::new);
        } else {
            this.rescoreVectorBuilder = null;
        }
    }

    public int k() {
        return k;
    }

    public int getNumCands() {
        return numCands;
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
            return new KnnSearchBuilder(field, querySupplier.get(), k, numCands, rescoreVectorBuilder, similarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(filterQueries)
                .innerHit(innerHitBuilder);
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
            return new KnnSearchBuilder(field, toSet::get, k, numCands, rescoreVectorBuilder, filterQueries, similarity).boost(boost)
                .queryName(queryName)
                .innerHit(innerHitBuilder);
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
            return new KnnSearchBuilder(field, queryVector, k, numCands, rescoreVectorBuilder, similarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(rewrittenQueries)
                .innerHit(innerHitBuilder);
        }
        return this;
    }

    public KnnVectorQueryBuilder toQueryBuilder() {
        if (queryVectorBuilder != null) {
            throw new IllegalArgumentException("missing rewrite");
        }
        return new KnnVectorQueryBuilder(field, queryVector, numCands, numCands, rescoreVectorBuilder, similarity).boost(boost)
            .queryName(queryName)
            .addFilterQueries(filterQueries);
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
            && Objects.equals(rescoreVectorBuilder, that.rescoreVectorBuilder)
            && Objects.equals(field, that.field)
            && Objects.equals(queryVector, that.queryVector)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(querySupplier, that.querySupplier)
            && Objects.equals(filterQueries, that.filterQueries)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(queryName, that.queryName)
            && boost == that.boost;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            k,
            numCands,
            querySupplier,
            queryVectorBuilder,
            rescoreVectorBuilder,
            similarity,
            Objects.hashCode(queryVector),
            Objects.hashCode(filterQueries),
            innerHitBuilder,
            queryName,
            boost
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);

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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalWriteable(queryVector);
        } else {
            out.writeFloatArray(queryVector.asFloatVector());
        }
        out.writeNamedWriteableCollection(filterQueries);
        out.writeFloat(boost);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeOptionalString(queryName);
        }
        if (out.getTransportVersion().before(TransportVersions.V_8_7_0) && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "cannot serialize [%s] to older node of version [%s]",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    out.getTransportVersion()
                )
            );
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            out.writeOptionalNamedWriteable(queryVectorBuilder);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalFloat(similarity);
        }
        if (out.getTransportVersion().onOrAfter(V_8_11_X)) {
            out.writeOptionalWriteable(innerHitBuilder);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.KNN_QUERY_RESCORE_OVERSAMPLE)) {
            out.writeOptionalWriteable(rescoreVectorBuilder);
        }
    }

    public static class Builder {

        private String field;
        private VectorData queryVector;
        private QueryVectorBuilder queryVectorBuilder;
        private Integer k;
        private Integer numCandidates;
        private Float similarity;
        private final List<QueryBuilder> filterQueries = new ArrayList<>();
        private String queryName;
        private float boost = DEFAULT_BOOST;
        private InnerHitBuilder innerHitBuilder;
        private RescoreVectorBuilder rescoreVectorBuilder;

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

        public Builder similarity(Float similarity) {
            this.similarity = similarity;
            return this;
        }

        public Builder rescoreVectorBuilder(RescoreVectorBuilder rescoreVectorBuilder) {
            this.rescoreVectorBuilder = rescoreVectorBuilder;
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
                rescoreVectorBuilder,
                similarity,
                innerHitBuilder,
                queryName,
                boost
            );
        }
    }
}
