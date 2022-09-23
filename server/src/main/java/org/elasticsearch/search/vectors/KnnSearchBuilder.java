/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Defines a kNN search to run in the search request.
 */
public class KnnSearchBuilder implements Writeable, ToXContentFragment, Rewriteable<KnnSearchBuilder> {
    private static final int NUM_CANDS_LIMIT = 10000;
    static final ParseField FIELD_FIELD = new ParseField("field");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    static final ParseField FILTER_FIELD = new ParseField("filter");
    static final ParseField BOOST_FIELD = AbstractQueryBuilder.BOOST_FIELD;

    private static final ConstructingObjectParser<KnnSearchBuilder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        @SuppressWarnings("unchecked")
        List<Float> vector = (List<Float>) args[1];
        float[] vectorArray = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            vectorArray[i] = vector.get(i);
        }
        return new KnnSearchBuilder((String) args[0], vectorArray, (int) args[2], (int) args[3]);
    });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFieldArray(
            KnnSearchBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
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
    final int k;
    final int numCands;
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
    public KnnSearchBuilder(String field, float[] queryVector, int k, int numCands) {
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
        this.field = field;
        this.queryVector = queryVector;
        this.k = k;
        this.numCands = numCands;
        this.filterQueries = new ArrayList<>();
    }

    public KnnSearchBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.k = in.readVInt();
        this.numCands = in.readVInt();
        this.queryVector = in.readFloatArray();
        this.filterQueries = in.readNamedWriteableList(QueryBuilder.class);
        this.boost = in.readFloat();
    }

    public int k() {
        return k;
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
            return new KnnSearchBuilder(field, queryVector, k, numCands).boost(boost).addFilterQueries(rewrittenQueries);
        }
        return this;
    }

    public KnnVectorQueryBuilder toQueryBuilder() {
        return new KnnVectorQueryBuilder(field, queryVector, numCands).boost(boost).addFilterQueries(filterQueries);
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
            && Objects.equals(filterQueries, that.filterQueries)
            && boost == that.boost;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, numCands, Arrays.hashCode(queryVector), Objects.hashCode(filterQueries), boost);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field)
            .field(K_FIELD.getPreferredName(), k)
            .field(NUM_CANDS_FIELD.getPreferredName(), numCands)
            .array(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);

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
        out.writeString(field);
        out.writeVInt(k);
        out.writeVInt(numCands);
        out.writeFloatArray(queryVector);
        out.writeNamedWriteableList(filterQueries);
        out.writeFloat(boost);
    }
}
