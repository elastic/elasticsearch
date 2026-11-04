/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Selects approximate candidates for an indexed {@code rank_vectors} field and
 * returns them with exact MaxSim scores computed from the original vectors.
 * Candidate selection is implementation-defined and may be approximate.
 */
public final class RankVectorsQueryBuilder extends LeafQueryBuilder<RankVectorsQueryBuilder> {

    public static final String NAME = "rank_vectors";
    public static final TransportVersion RANK_VECTORS_QUERY = TransportVersion.fromName("rank_vectors_query");

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDIDATES_FIELD = new ParseField("num_candidates");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    private static final int MAX_CANDIDATES = 10_000;
    private static final long MAX_EXACT_DOT_PRODUCTS = 250_000_000L;

    private static final ConstructingObjectParser<RankVectorsQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new RankVectorsQueryBuilder((String) args[0], (float[][]) args[1], (int) args[2], (int) args[3])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseQueryVectors(parser),
            QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDIDATES_FIELD);
        PARSER.declareFieldArray(
            RankVectorsQueryBuilder::addFilterQueries,
            (parser, context) -> parseInnerQueryBuilder(parser),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        declareStandardFields(PARSER);
    }

    private final String fieldName;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;
    private final List<QueryBuilder> filterQueries = new ArrayList<>();

    public RankVectorsQueryBuilder(String fieldName, float[][] queryVectors, int k, int numCandidates) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a [" + FIELD_FIELD.getPreferredName() + "]");
        }
        validate(queryVectors, k, numCandidates);
        this.fieldName = fieldName;
        this.queryVectors = deepCopy(queryVectors);
        this.k = k;
        this.numCandidates = numCandidates;
    }

    public RankVectorsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryVectors = in.readArray(StreamInput::readFloatArray, float[][]::new);
        this.k = in.readVInt();
        this.numCandidates = in.readVInt();
        validate(queryVectors, k, numCandidates);
        this.filterQueries.addAll(readQueries(in));
    }

    public static RankVectorsQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String getFieldName() {
        return fieldName;
    }

    public float[][] getQueryVectors() {
        return deepCopy(queryVectors);
    }

    public int getK() {
        return k;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public List<QueryBuilder> getFilterQueries() {
        return List.copyOf(filterQueries);
    }

    public RankVectorsQueryBuilder addFilterQuery(QueryBuilder filterQuery) {
        filterQueries.add(Objects.requireNonNull(filterQuery));
        return this;
    }

    public RankVectorsQueryBuilder addFilterQueries(List<QueryBuilder> filters) {
        filters.forEach(this::addFilterQuery);
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return RANK_VECTORS_QUERY;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeArray(StreamOutput::writeFloatArray, queryVectors);
        out.writeVInt(k);
        out.writeVInt(numCandidates);
        writeQueries(out, filterQueries);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.startArray(QUERY_VECTOR_FIELD.getPreferredName());
        for (float[] vector : queryVectors) {
            builder.startArray();
            for (float value : vector) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
        if (filterQueries.isEmpty() == false) {
            builder.startArray(FILTER_FIELD.getPreferredName());
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewrittenFilters = new ArrayList<>(filterQueries.size());
        for (QueryBuilder filterQuery : filterQueries) {
            QueryBuilder rewritten = filterQuery.rewrite(context);
            if (rewritten instanceof MatchNoneQueryBuilder) {
                return rewritten;
            }
            changed |= rewritten != filterQuery;
            rewrittenFilters.add(rewritten);
        }
        if (changed) {
            return new RankVectorsQueryBuilder(fieldName, queryVectors, k, numCandidates).addFilterQueries(rewrittenFilters)
                .boost(boost)
                .queryName(queryName);
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            return Queries.NO_DOCS_INSTANCE;
        }
        if (fieldType instanceof RankVectorsFieldMapper.RankVectorsFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + RankVectorsFieldMapper.CONTENT_TYPE + "] fields"
            );
        }
        RankVectorsFieldMapper.RankVectorsFieldType rankVectorsFieldType = (RankVectorsFieldMapper.RankVectorsFieldType) fieldType;
        if (context.nestedLookup().getNestedParent(fieldType.name()) != null) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries do not support nested [" + RankVectorsFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        List<Query> filters = new ArrayList<>(filterQueries.size() + 1);
        for (QueryBuilder filterQuery : filterQueries) {
            filters.add(filterQuery.toQuery(context));
        }
        if (context.getAliasFilter() != null) {
            try (Releasable ignored = context.nestedScope().unwindTo(null)) {
                filters.add(context.getAliasFilter().toQuery(context));
            }
        }
        Query filter = buildFilterQuery(filters);
        return rankVectorsFieldType.maxSimQuery(queryVectors, k, numCandidates, filter);
    }

    @Override
    protected boolean doEquals(RankVectorsQueryBuilder other) {
        return fieldName.equals(other.fieldName)
            && Arrays.deepEquals(queryVectors, other.queryVectors)
            && k == other.k
            && numCandidates == other.numCandidates
            && filterQueries.equals(other.filterQueries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.deepHashCode(queryVectors), k, numCandidates, filterQueries);
    }

    private static Query buildFilterQuery(List<Query> filters) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Query filter : filters) {
            if (filter.getClass() != MatchAllDocsQuery.class) {
                builder.add(filter, BooleanClause.Occur.FILTER);
            }
        }
        BooleanQuery query = builder.build();
        return query.clauses().isEmpty() ? null : query;
    }

    static float[][] parseQueryVectors(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ParsingException(parser.getTokenLocation(), "[" + QUERY_VECTOR_FIELD.getPreferredName() + "] must be an array");
        }
        List<float[]> vectors = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + QUERY_VECTOR_FIELD.getPreferredName() + "] must contain arrays of numbers"
                );
            }
            List<Float> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + QUERY_VECTOR_FIELD.getPreferredName() + "] vectors must contain only numbers"
                    );
                }
                values.add(parser.floatValue());
            }
            float[] vector = new float[values.size()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = values.get(i);
            }
            vectors.add(vector);
        }
        return vectors.toArray(float[][]::new);
    }

    private static void validate(float[][] queryVectors, int k, int numCandidates) {
        if (queryVectors == null || queryVectors.length == 0) {
            throw new IllegalArgumentException("[" + QUERY_VECTOR_FIELD.getPreferredName() + "] must not be empty");
        }
        if (queryVectors.length > RankVectorsFieldMapper.MAX_MUVERA_VECTORS) {
            throw new IllegalArgumentException(
                "["
                    + QUERY_VECTOR_FIELD.getPreferredName()
                    + "] vector count ["
                    + queryVectors.length
                    + "] exceeds the maximum of ["
                    + RankVectorsFieldMapper.MAX_MUVERA_VECTORS
                    + "]"
            );
        }
        for (int i = 0; i < queryVectors.length; i++) {
            if (queryVectors[i] == null || queryVectors[i].length == 0) {
                throw new IllegalArgumentException("[" + QUERY_VECTOR_FIELD.getPreferredName() + "] vector [" + i + "] must not be empty");
            }
            for (float value : queryVectors[i]) {
                if (Float.isFinite(value) == false) {
                    throw new IllegalArgumentException("[" + QUERY_VECTOR_FIELD.getPreferredName() + "] contains a non-finite value");
                }
            }
        }
        if (k < 1) {
            throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (numCandidates < k) {
            throw new IllegalArgumentException(
                "[" + NUM_CANDIDATES_FIELD.getPreferredName() + "] must be greater than or equal to [" + K_FIELD.getPreferredName() + "]"
            );
        }
        if (numCandidates > MAX_CANDIDATES) {
            throw new IllegalArgumentException("[" + NUM_CANDIDATES_FIELD.getPreferredName() + "] cannot exceed [" + MAX_CANDIDATES + "]");
        }
        validateRescoreWork(queryVectors.length, numCandidates);
    }

    /** Rejects a MaxSim rescore whose worst-case dot-product count exceeds the query safety limit. */
    public static void validateRescoreWork(int queryVectorCount, int numCandidates) {
        long exactDotProducts = Math.multiplyExact(
            Math.multiplyExact((long) numCandidates, RankVectorsFieldMapper.MAX_MUVERA_VECTORS),
            queryVectorCount
        );
        if (exactDotProducts > MAX_EXACT_DOT_PRODUCTS) {
            throw new IllegalArgumentException(
                "exact MaxSim work ["
                    + exactDotProducts
                    + " dot products] exceeds the maximum of ["
                    + MAX_EXACT_DOT_PRODUCTS
                    + "]; reduce [num_candidates] or the number of query vectors"
            );
        }
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
