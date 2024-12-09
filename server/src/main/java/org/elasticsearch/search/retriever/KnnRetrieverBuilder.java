/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
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
    private final Supplier<float[]> queryVector;
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
        this.field = field;
        this.queryVector = queryVector != null ? () -> queryVector : null;
        this.queryVectorBuilder = queryVectorBuilder;
        this.k = k;
        this.numCands = numCands;
        this.similarity = similarity;
    }

    private KnnRetrieverBuilder(KnnRetrieverBuilder clone, Supplier<float[]> queryVector, QueryVectorBuilder queryVectorBuilder) {
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.field = clone.field;
        this.k = clone.k;
        this.numCands = clone.numCands;
        this.similarity = clone.similarity;
        this.retrieverName = clone.retrieverName;
        this.preFilterQueryBuilders = clone.preFilterQueryBuilders;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        var rewrittenFilters = rewritePreFilters(ctx);
        if (rewrittenFilters != preFilterQueryBuilders) {
            var rewritten = new KnnRetrieverBuilder(this, queryVector, queryVectorBuilder);
            rewritten.preFilterQueryBuilders = rewrittenFilters;
            return rewritten;
        }

        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> {
                queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
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
                }));
            });
            return new KnnRetrieverBuilder(this, () -> toSet.get(), null);
        }
        return super.rewrite(ctx);
    }

    @Override
    public QueryBuilder topDocsQuery() {
        assert queryVector != null : "query vector must be materialized at this point";
        assert rankDocs != null : "rankDocs should have been materialized by now";
        var rankDocsQuery = new RankDocsQueryBuilder(rankDocs, null, true);
        if (preFilterQueryBuilders.isEmpty()) {
            return rankDocsQuery.queryName(retrieverName);
        }
        BoolQueryBuilder res = new BoolQueryBuilder().must(rankDocsQuery);
        preFilterQueryBuilders.forEach(res::filter);
        return res.queryName(retrieverName);
    }

    @Override
    public QueryBuilder explainQuery() {
        assert queryVector != null : "query vector must be materialized at this point";
        assert rankDocs != null : "rankDocs should have been materialized by now";
        var rankDocsQuery = new RankDocsQueryBuilder(
            rankDocs,
            new QueryBuilder[] { new ExactKnnQueryBuilder(VectorData.fromFloats(queryVector.get()), field, similarity) },
            true
        );
        if (preFilterQueryBuilders.isEmpty()) {
            return rankDocsQuery.queryName(retrieverName);
        }
        BoolQueryBuilder res = new BoolQueryBuilder().must(rankDocsQuery);
        preFilterQueryBuilders.forEach(res::filter);
        return res.queryName(retrieverName);
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        assert queryVector != null : "query vector must be materialized at this point.";
        KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder(
            field,
            VectorData.fromFloats(queryVector.get()),
            null,
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

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);

        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector.get());
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
            && ((queryVector == null && that.queryVector == null)
                || (queryVector != null && that.queryVector != null && Arrays.equals(queryVector.get(), that.queryVector.get())))
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int doHashCode() {
        int result = Objects.hash(field, queryVectorBuilder, k, numCands, similarity);
        result = 31 * result + Arrays.hashCode(queryVector != null ? queryVector.get() : null);
        return result;
    }

    // ---- END TESTING ----
}
