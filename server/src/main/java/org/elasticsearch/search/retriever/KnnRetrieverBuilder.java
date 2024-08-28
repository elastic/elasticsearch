/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A knn retriever is used to represent a knn search
 * with some elements to specify parameters for that knn search.
 */
public final class KnnRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "knn";
    public static final NodeFeature KNN_RETRIEVER_SUPPORTED = new NodeFeature("knn_retriever_supported");

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<KnnRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "knn",
        args -> {
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
            return new KnnRetrieverBuilder(
                (String) args[0],
                vectorArray,
                (QueryVectorBuilder) args[2],
                (int) args[3],
                (int) args[4],
                (Float) args[5]
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(optionalConstructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static KnnRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(KNN_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    private final String field;
    private final float[] queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final int k;
    private final int numCands;
    private final Float similarity;

    public KnnRetrieverBuilder(
        String field,
        float[] queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float similarity
    ) {
        this.field = field;
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.k = k;
        this.numCands = numCands;
        this.similarity = similarity;
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        assert rankDocs != null : "{rankDocs} should have been materialized at this point";

        BoolQueryBuilder knnTopResultsQuery = new BoolQueryBuilder().filter(new RankDocsQueryBuilder(rankDocs))
            .should(new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector), field, similarity));
        preFilterQueryBuilders.forEach(knnTopResultsQuery::filter);
        return knnTopResultsQuery;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder(
            field,
            VectorData.fromFloats(queryVector),
            queryVectorBuilder,
            k,
            numCands,
            similarity
        );
        if (preFilterQueryBuilders != null) {
            knnSearchBuilder.addFilterQueries(preFilterQueryBuilders);
        }
        if (retrieverName != null) {
            knnSearchBuilder.queryName(retrieverName);
        }
        List<KnnSearchBuilder> knnSearchBuilders = new ArrayList<>(searchSourceBuilder.knnSearch());
        knnSearchBuilders.add(knnSearchBuilder);
        searchSourceBuilder.knnSearch(knnSearchBuilders);
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);

        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }

        if (queryVectorBuilder != null) {
            builder.field(QUERY_VECTOR_BUILDER_FIELD.getPreferredName(), queryVectorBuilder);
        }

        if (similarity != null) {
            builder.field(VECTOR_SIMILARITY.getPreferredName(), similarity);
        }
    }

    @Override
    public boolean doEquals(Object o) {
        KnnRetrieverBuilder that = (KnnRetrieverBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(field, that.field)
            && Arrays.equals(queryVector, that.queryVector)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int doHashCode() {
        int result = Objects.hash(field, queryVectorBuilder, k, numCands, similarity);
        result = 31 * result + Arrays.hashCode(queryVector);
        return result;
    }

    // ---- END TESTING ----
}
