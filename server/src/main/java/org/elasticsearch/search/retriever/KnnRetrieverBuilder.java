/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class KnnRetrieverBuilder extends RetrieverBuilder<KnnRetrieverBuilder> {

    public static final String NAME = "knn";

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

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
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
    }

    @Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder(field, queryVector, queryVectorBuilder, k, numCands, similarity);
        if (preFilterQueryBuilders != null) {
            knnSearchBuilder.addFilterQueries(preFilterQueryBuilders);
        }
        searchSourceBuilder.knnSearch(List.of(knnSearchBuilder));
    }
}
