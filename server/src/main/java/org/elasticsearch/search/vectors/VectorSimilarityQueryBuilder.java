/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class VectorSimilarityQueryBuilder extends AbstractQueryBuilder<VectorSimilarityQueryBuilder> {
    public static final String NAME = "vector_similarity";
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField QUERY_VECTOR = new ParseField("query_vector");
    private static final ParseField NUM_CANDIDATES = new ParseField("num_candidates");
    private static final ParseField SIMILARITY = new ParseField("similarity");
    private static final ParseField FILTER = new ParseField("filter");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<VectorSimilarityQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME + "_parser",
        args -> {
            List<Float> vector = (List<Float>) args[1];
            final float[] vectorArray;
            vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
            return new VectorSimilarityQueryBuilder((String) args[0], vectorArray, (Integer) args[2], (Float) args[3]);
        }
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareFloatArray(ConstructingObjectParser.constructorArg(), QUERY_VECTOR);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CANDIDATES);
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), SIMILARITY);
        PARSER.declareFieldArray(
            VectorSimilarityQueryBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        AbstractQueryBuilder.declareStandardFields(PARSER);
    }

    public static VectorSimilarityQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final float[] queryVector;
    private final int numCandidates;
    private final float similarity;
    private final List<QueryBuilder> filters;

    public VectorSimilarityQueryBuilder(String field, float[] queryVector, int numCandidates, float similarity) {
        this.field = Objects.requireNonNull(field, Strings.format("[%s] must not be null", FIELD.getPreferredName()));
        this.queryVector = Objects.requireNonNull(queryVector, Strings.format("[%s] must not be null", QUERY_VECTOR.getPreferredName()));
        if (numCandidates <= 0) {
            throw new IllegalArgumentException(Strings.format("[%s] must be greater than 0", NUM_CANDIDATES.getPreferredName()));
        }
        this.numCandidates = numCandidates;
        this.similarity = similarity;
        this.filters = new ArrayList<>();
    }

    public VectorSimilarityQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.queryVector = in.readFloatArray();
        this.numCandidates = in.readVInt();
        this.similarity = in.readFloat();
        this.filters = in.readNamedWriteableList(QueryBuilder.class);
    }

    public String getField() {
        return field;
    }

    public float[] getQueryVector() {
        return queryVector;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public float getSimilarity() {
        return similarity;
    }

    public List<QueryBuilder> getFilters() {
        return filters;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    public VectorSimilarityQueryBuilder addFilterQuery(QueryBuilder filterQuery) {
        Objects.requireNonNull(filterQuery);
        this.filters.add(filterQuery);
        return this;
    }

    public VectorSimilarityQueryBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filters.addAll(filterQueries);
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(queryVector);
        out.writeVInt(numCandidates);
        out.writeFloat(similarity);
        out.writeNamedWriteableList(filters);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        boostAndQueryNameToXContent(builder);
        builder.field(FIELD.getPreferredName(), field);
        builder.field(QUERY_VECTOR.getPreferredName(), queryVector);
        builder.field(NUM_CANDIDATES.getPreferredName(), numCandidates);
        builder.field(SIMILARITY.getPreferredName(), similarity);
        if (filters.size() > 0) {
            builder.startArray(FILTER.getPreferredName());
            for (QueryBuilder filterQuery : filters) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + field + "] does not exist in the mapping");
        }

        if (fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        DenseVectorFieldMapper.DenseVectorFieldType vectorFieldType = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder query : this.filters) {
            builder.add(query.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;
        return vectorFieldType.createVectorSimilarity(queryVector, similarity, numCandidates, filterQuery);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (filters.isEmpty()) {
            return this;
        }
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filters.size());
        for (QueryBuilder query : filters) {
            QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed == false) {
            return this;
        }
        return new VectorSimilarityQueryBuilder(field, queryVector, numCandidates, similarity).addFilterQueries(rewrittenQueries)
            .queryName(queryName)
            .boost(boost);
    }

    @Override
    protected boolean doEquals(VectorSimilarityQueryBuilder other) {
        return Objects.equals(field, other.field)
            && Objects.equals(filters, other.filters)
            && Arrays.equals(queryVector, other.queryVector)
            && similarity == other.similarity
            && numCandidates == other.numCandidates;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.hashCode(queryVector), numCandidates, similarity, filters);
    }
}
