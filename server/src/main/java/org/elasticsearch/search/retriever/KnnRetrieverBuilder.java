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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
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

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField K_FIELD = new ParseField("k");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY = new ParseField("similarity");
    public static final ParseField RESCORE_VECTOR_FIELD = new ParseField("rescore_vector");

    public static final ConstructingObjectParser<KnnRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "knn",
        args -> {
            return new KnnRetrieverBuilder(
                (String) args[0],
                (VectorData) args[1],
                (QueryVectorBuilder) args[2],
                (int) args[3],
                (int) args[4],
                (Float) args[5],
                (RescoreVectorBuilder) args[7],
                (Float) args[6]
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> VectorData.parseXContent(p),
            QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER
        );
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        PARSER.declareInt(constructorArg(), K_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VISIT_PERCENTAGE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> RescoreVectorBuilder.fromXContent(p),
            RESCORE_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public static KnnRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private final String field;
    private final Supplier<VectorData> queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final int k;
    private final int numCands;
    private final Float visitPercentage;
    private final RescoreVectorBuilder rescoreVectorBuilder;
    private final Float similarity;

    public KnnRetrieverBuilder(
        String field,
        float[] queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        this(field, VectorData.fromFloats(queryVector), queryVectorBuilder, k, numCands, visitPercentage, rescoreVectorBuilder, similarity);
    }

    public KnnRetrieverBuilder(
        String field,
        VectorData queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int k,
        int numCands,
        Float visitPercentage,
        RescoreVectorBuilder rescoreVectorBuilder,
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
        this.visitPercentage = visitPercentage;
        this.similarity = similarity;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
    }

    private KnnRetrieverBuilder(KnnRetrieverBuilder clone, Supplier<VectorData> queryVector, QueryVectorBuilder queryVectorBuilder) {
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.field = clone.field;
        this.k = clone.k;
        this.numCands = clone.numCands;
        this.visitPercentage = clone.visitPercentage;
        this.similarity = clone.similarity;
        this.retrieverName = clone.retrieverName;
        this.preFilterQueryBuilders = clone.preFilterQueryBuilders;
        this.rescoreVectorBuilder = clone.rescoreVectorBuilder;
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
            SetOnce<VectorData> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> {
                queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
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
                    toSet.set(VectorData.fromFloats(v));
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
            new QueryBuilder[] { new ExactKnnQueryBuilder(queryVector.get(), field, similarity) },
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
            queryVector.get(),
            null,
            k,
            numCands,
            visitPercentage,
            rescoreVectorBuilder,
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

    RescoreVectorBuilder rescoreVectorBuilder() {
        return rescoreVectorBuilder;
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);

        if (visitPercentage != null) {
            builder.field(VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentage);
        }

        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector.get());
        }

        if (queryVectorBuilder != null) {
            builder.field(QUERY_VECTOR_BUILDER_FIELD.getPreferredName(), queryVectorBuilder);
        }

        if (similarity != null) {
            builder.field(VECTOR_SIMILARITY.getPreferredName(), similarity);
        }

        if (rescoreVectorBuilder != null) {
            builder.field(RESCORE_VECTOR_FIELD.getPreferredName(), rescoreVectorBuilder);
        }
    }

    @Override
    public boolean doEquals(Object o) {
        KnnRetrieverBuilder that = (KnnRetrieverBuilder) o;
        return k == that.k
            && numCands == that.numCands
            && Objects.equals(visitPercentage, that.visitPercentage)
            && Objects.equals(field, that.field)
            && Objects.equals(queryVector != null ? queryVector.get() : null, that.queryVector != null ? that.queryVector.get() : null)
            && Objects.equals(queryVectorBuilder, that.queryVectorBuilder)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rescoreVectorBuilder, that.rescoreVectorBuilder);
    }

    @Override
    public int doHashCode() {
        int result = Objects.hash(field, queryVectorBuilder, k, numCands, visitPercentage, rescoreVectorBuilder, similarity);
        result = 31 * result + Objects.hashCode(queryVector != null ? queryVector.get() : null);
        return result;
    }

    // ---- END TESTING ----
}
