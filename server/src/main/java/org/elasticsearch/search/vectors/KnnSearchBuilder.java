/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Defines a kNN search to run in the search request.
 */
public class KnnSearchBuilder implements Writeable, ToXContentFragment, Rewriteable<KnnSearchBuilder> {
    private static final int NUM_CANDS_LIMIT = 10000;
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField BOOST_FIELD = AbstractQueryBuilder.BOOST_FIELD;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KnnSearchBuilder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        // TODO optimize parsing for when BYTE values are provided
        List<Float> vector = (List<Float>) args[1];
        final float[] vectorArray;
        if (vector != null) {
            vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
        } else {
            vectorArray = null;
        }
        return new KnnSearchBuilder(
            (String) args[0],
            vectorArray,
            (QueryVectorBuilder) args[4],
            (int) args[2],
            (int) args[3],
            (Float) args[5]
        );
    });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(optionalConstructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY);
        PARSER.declareFieldArray(
            KnnSearchBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareFloat(KnnSearchBuilder::boost, BOOST_FIELD);
    }

    public static KnnSearchBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    final String field;
    final float[] queryVector;
    final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> querySupplier;
    final int k;
    final int numCands;
    final Float similarity;
    final List<QueryBuilder> filterQueries;
    float boost = AbstractQueryBuilder.DEFAULT_BOOST;

    /**
     * Defines a kNN search.
     *
     * @param field       the name of the vector field to search against
     * @param queryVector the query vector
     * @param k           the final number of nearest neighbors to return as top hits
     * @param numCands    the number of nearest neighbor candidates to consider per shard
     */
    public KnnSearchBuilder(String field, float[] queryVector, int k, int numCands, Float similarity) {
        this(field, Objects.requireNonNull(queryVector, format("[%s] cannot be null", QUERY_VECTOR_FIELD)), null, k, numCands, similarity);
    }

    /**
     * Defines a kNN search where the query vector will be provided by the queryVectorBuilder
     * @param field              the name of the vector field to search against
     * @param queryVectorBuilder the query vector builder
     * @param k                  the final number of nearest neighbors to return as top hits
     * @param numCands           the number of nearest neighbor candidates to consider per shard
     */
    public KnnSearchBuilder(String field, QueryVectorBuilder queryVectorBuilder, int k, int numCands, Float similarity) {
        this(
            field,
            null,
            Objects.requireNonNull(queryVectorBuilder, format("[%s] cannot be null", QUERY_VECTOR_BUILDER_FIELD.getPreferredName())),
            k,
            numCands,
            similarity
        );
    }

    private KnnSearchBuilder(
        String field,
        float[] queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float similarity
    ) {
        if (k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCands < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than " + "[" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (numCands > NUM_CANDS_LIMIT) {
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
        this.queryVector = queryVector == null ? new float[0] : queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.k = k;
        this.numCands = numCands;
        this.filterQueries = new ArrayList<>();
        this.querySupplier = null;
        this.similarity = similarity;
    }

    private KnnSearchBuilder(
        String field,
        Supplier<float[]> querySupplier,
        int k,
        int numCands,
        List<QueryBuilder> filterQueries,
        Float similarity
    ) {
        this.field = field;
        this.queryVector = new float[0];
        this.queryVectorBuilder = null;
        this.k = k;
        this.numCands = numCands;
        this.filterQueries = filterQueries;
        this.querySupplier = querySupplier;
        this.similarity = similarity;
    }

    public KnnSearchBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        this.queryVector = in.readFloatArray();
        this.filterQueries = in.readNamedWriteableList(QueryBuilder.class);
        this.boost = in.readFloat();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        } else {
            this.queryVectorBuilder = null;
        }
        this.querySupplier = null;
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            this.similarity = in.readOptionalFloat();
        } else {
            this.similarity = null;
        }
    }

    public int k() {
        return k;
    }

    public QueryVectorBuilder getQueryVectorBuilder() {
        return queryVectorBuilder;
    }

    // for testing only
    public float[] getQueryVector() {
        return queryVector;
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
     * Set a boost to apply to the kNN search scores.
     */
    public KnnSearchBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public KnnSearchBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (querySupplier != null) {
            if (querySupplier.get() == null) {
                return this;
            }
            return new KnnSearchBuilder(field, querySupplier.get(), k, numCands, similarity).boost(boost).addFilterQueries(filterQueries);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> queryVectorBuilder.buildVector(c, l.wrapFailure((ll, v) -> {
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
            return new KnnSearchBuilder(field, toSet::get, k, numCands, filterQueries, similarity).boost(boost);
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
            return new KnnSearchBuilder(field, queryVector, k, numCands, similarity).boost(boost).addFilterQueries(rewrittenQueries);
        }
        return this;
    }

    public KnnVectorQueryBuilder toQueryBuilder() {
        if (queryVectorBuilder != null) {
            throw new IllegalArgumentException("missing rewrite");
        }
        return new KnnVectorQueryBuilder(field, queryVector, numCands, similarity).boost(boost).addFilterQueries(filterQueries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnSearchBuilder that = (KnnSearchBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(field, that.field)
            && Arrays.equals(queryVector, that.queryVector)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(querySupplier, that.querySupplier)
            && Objects.equals(filterQueries, that.filterQueries)
            && Objects.equals(similarity, that.similarity)
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
            similarity,
            Arrays.hashCode(queryVector),
            Objects.hashCode(filterQueries),
            boost
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field)
            .field(K_FIELD.getPreferredName(), k)
            .field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        } else {
            builder.array(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
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

        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            builder.field(BOOST_FIELD.getPreferredName(), boost);
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
        out.writeFloatArray(queryVector);
        out.writeNamedWriteableList(filterQueries);
        out.writeFloat(boost);
        if (out.getTransportVersion().before(TransportVersion.V_8_7_0) && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "cannot serialize [%s] to older node of version [%s]",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    out.getTransportVersion()
                )
            );
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalNamedWriteable(queryVectorBuilder);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeOptionalFloat(similarity);
        }
    }
}
