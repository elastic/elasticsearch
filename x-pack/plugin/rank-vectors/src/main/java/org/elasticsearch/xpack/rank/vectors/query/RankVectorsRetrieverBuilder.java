/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Retrieves documents from an indexed {@code rank_vectors} field and returns
 * exact MaxSim scores for the selected candidates.
 */
public final class RankVectorsRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = RankVectorsQueryBuilder.NAME;

    private static final ConstructingObjectParser<RankVectorsRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(
            NAME,
            args -> new RankVectorsRetrieverBuilder((String) args[0], (float[][]) args[1], (int) args[2], (int) args[3])
        );

    static {
        PARSER.declareString(constructorArg(), RankVectorsQueryBuilder.FIELD_FIELD);
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> RankVectorsQueryBuilder.parseQueryVectors(parser),
            RankVectorsQueryBuilder.QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareInt(constructorArg(), RankVectorsQueryBuilder.K_FIELD);
        PARSER.declareInt(constructorArg(), RankVectorsQueryBuilder.NUM_CANDIDATES_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private final String fieldName;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;

    public RankVectorsRetrieverBuilder(String fieldName, float[][] queryVectors, int k, int numCandidates) {
        RankVectorsQueryBuilder queryBuilder = new RankVectorsQueryBuilder(fieldName, queryVectors, k, numCandidates);
        this.fieldName = queryBuilder.getFieldName();
        this.queryVectors = queryBuilder.getQueryVectors();
        this.k = queryBuilder.getK();
        this.numCandidates = queryBuilder.getNumCandidates();
    }

    private RankVectorsRetrieverBuilder(RankVectorsRetrieverBuilder source, List<QueryBuilder> filters) {
        this(source.fieldName, source.queryVectors, source.k, source.numCandidates);
        this.preFilterQueryBuilders = filters;
        this.retrieverName = source.retrieverName;
        this.minScore = source.minScore;
    }

    public static RankVectorsRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) {
        return PARSER.apply(parser, context);
    }

    public RankVectorsRetrieverBuilder addFilterQuery(QueryBuilder filterQuery) {
        preFilterQueryBuilders.add(Objects.requireNonNull(filterQuery));
        return this;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        List<QueryBuilder> rewrittenFilters = rewritePreFilters(ctx);
        if (rewrittenFilters != preFilterQueryBuilders) {
            return new RankVectorsRetrieverBuilder(this, rewrittenFilters);
        }
        return this;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        return queryBuilder();
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(queryBuilder()));
        if (minScore != null) {
            searchSourceBuilder.minScore(minScore);
        }
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RankVectorsQueryBuilder.FIELD_FIELD.getPreferredName(), fieldName);
        builder.startArray(RankVectorsQueryBuilder.QUERY_VECTOR_FIELD.getPreferredName());
        for (float[] vector : queryVectors) {
            builder.startArray();
            for (float value : vector) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();
        builder.field(RankVectorsQueryBuilder.K_FIELD.getPreferredName(), k);
        builder.field(RankVectorsQueryBuilder.NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
    }

    @Override
    protected boolean doEquals(Object o) {
        RankVectorsRetrieverBuilder other = (RankVectorsRetrieverBuilder) o;
        return fieldName.equals(other.fieldName)
            && Arrays.deepEquals(queryVectors, other.queryVectors)
            && k == other.k
            && numCandidates == other.numCandidates;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.deepHashCode(queryVectors), k, numCandidates);
    }

    private RankVectorsQueryBuilder queryBuilder() {
        return new RankVectorsQueryBuilder(fieldName, queryVectors, k, numCandidates).addFilterQueries(preFilterQueryBuilders)
            .queryName(retrieverName);
    }
}
