/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversification;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class ResultDiversificationRetrieverBuilder extends CompoundRetrieverBuilder<ResultDiversificationRetrieverBuilder> {

    public static final String DIVERSIFICATION_TYPE_MMR = "mmr";
    public static final Float DEFAULT_LAMBDA_VALUE = 0.7f;
    public static final Integer DEFAULT_NUM_CANDIDATES = 10;

    public static final String NAME = "diversify";
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_FIELD = new ParseField("query_vector");
    public static final ParseField LAMBDA_FIELD = new ParseField("lambda");
    public static final ParseField NUM_CANDIDATES = new ParseField("num_candidates");

    public static class RankDocWithSearchHit extends RankDoc {
        private final SearchHit hit;

        public RankDocWithSearchHit(int doc, float score, int shardIndex, SearchHit hit) {
            super(doc, score, shardIndex);
            this.hit = hit;
        }

        public SearchHit hit() {
            return hit;
        }
    }

    static final ConstructingObjectParser<ResultDiversificationRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(NAME, false, args -> {
            String diversificationType = (String) args[1];
            String diversificationField = (String) args[2];
            int rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[3];
            float[] queryVector = args[4] == null ? null : (float[]) args[4];
            Float lambda = args[5] == null ? DEFAULT_LAMBDA_VALUE : (Float) args[5];
            int numCandidates = args[6] == null ? DEFAULT_NUM_CANDIDATES : (Integer) args[6];
            return new ResultDiversificationRetrieverBuilder(
                RetrieverSource.from((RetrieverBuilder) args[0]),
                diversificationType,
                diversificationField,
                rankWindowSize,
                queryVector,
                lambda,
                numCandidates
            );
        });

    static {
        PARSER.declareNamedObject(constructorArg(), (parser, context, n) -> {
            RetrieverBuilder innerRetriever = parser.namedObject(RetrieverBuilder.class, n, context);
            context.trackRetrieverUsage(innerRetriever);
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareFloatArray(optionalConstructorArg(), QUERY_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), LAMBDA_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), NUM_CANDIDATES);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private final String diversificationType;
    private final String diversificationField;
    private final float[] queryVector;
    private final Float lambda;
    private final int numCandidates;
    private ResultDiversificationContext diversificationContext = null;

    ResultDiversificationRetrieverBuilder(
        RetrieverSource innerRetriever,
        String diversificationType,
        String diversificationField,
        int rankWindowSize,
        float[] queryVector,
        @Nullable Float lambda,
        int numCandidates
    ) {
        super(List.of(innerRetriever), rankWindowSize);
        this.diversificationType = diversificationType;
        this.diversificationField = diversificationField;
        this.queryVector = queryVector;
        this.lambda = lambda;
        this.numCandidates = numCandidates;
    }

    @Override
    protected ResultDiversificationRetrieverBuilder clone(
        List<RetrieverSource> newChildRetrievers,
        List<QueryBuilder> newPreFilterQueryBuilders
    ) {
        assert newChildRetrievers.size() == 1 : "ResultDiversificationRetrieverBuilder must have a single child retriever";
        return new ResultDiversificationRetrieverBuilder(
            newChildRetrievers.getFirst(),
            diversificationType,
            diversificationField,
            rankWindowSize,
            queryVector,
            lambda,
            numCandidates
        );
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        // ensure the type is one we know of - at the moment, only "mmr" is valid
        if (diversificationType.equals(DIVERSIFICATION_TYPE_MMR) == false) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "[%s] diversification type must be set to `[%s]`", getName(), DIVERSIFICATION_TYPE_MMR),
                validationException
            );
        }

        // if MMR, ensure we have a lambda between 0.0 and 1.0
        if (diversificationType.equals(DIVERSIFICATION_TYPE_MMR) && (lambda < 0.0 || lambda > 1.0)) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "[%s] MMR result diversification must have a lambda between 0.0 and 1.0", getName()),
                validationException
            );
        }

        return validationException;
    }

    @Override
    protected RetrieverBuilder doRewrite(QueryRewriteContext ctx) {
        IndexVersion indexVersion = ctx.getIndexSettings().getIndexVersionCreated();
        Mapper mapper = ctx.getMappingLookup().getMapper(diversificationField);
        if (mapper instanceof DenseVectorFieldMapper == false) {
            throw new IllegalArgumentException("[" + diversificationField + "] is not a dense vector field");
        }

        if (diversificationType.equals(DIVERSIFICATION_TYPE_MMR)) {
            diversificationContext = new MMRResultDiversificationContext(
                diversificationField,
                lambda,
                numCandidates,
                new VectorData(queryVector),
                (DenseVectorFieldMapper) mapper,
                indexVersion,
                null
            );
        } else {
            // should not happen
            throw new IllegalArgumentException("Unknown diversification type [" + diversificationType + "]");
        }

        return this;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        return super.finalizeSourceBuilder(sourceBuilder).docValueField(diversificationField);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        // must have the combined result set
        assert rankResults.size() == 1;
        assert diversificationContext != null : "diversificationContext should be set before combining results";

        ScoreDoc[] scoreDocs = rankResults.getFirst();
        if (scoreDocs == null || scoreDocs.length == 0) {
            // might happen in the case where we have no results
            return new RankDoc[0];
        }

        assert scoreDocs[0] instanceof RankDocWithSearchHit : "expected results to be of type RankDocWithSearchHit";

        // gather and set the query vectors
        // and create our intermediate results set
        RankDoc[] results = new RankDoc[scoreDocs.length];
        Map<Integer, VectorData> fieldVectors = new HashMap<>();
        for (int i = 0; i < scoreDocs.length; i++) {
            RankDocWithSearchHit asRankDoc = (RankDocWithSearchHit) scoreDocs[i];
            results[i] = asRankDoc;

            var field = asRankDoc.hit().getFields().getOrDefault(diversificationField, null);
            if (field != null) {
                var fieldValue = field.getValue();
                if (fieldValue instanceof float[]) {
                    fieldVectors.put(asRankDoc.doc, new VectorData((float[]) field.getValue()));
                } else if (fieldValue instanceof byte[]) {
                    fieldVectors.put(asRankDoc.doc, new VectorData((byte[]) field.getValue()));
                }
            }
        }
        diversificationContext.setFieldVectors(fieldVectors);

        try {
            if (diversificationType.equals(DIVERSIFICATION_TYPE_MMR)) {
                MMRResultDiversification diversification = new MMRResultDiversification();
                results = diversification.diversify(results, diversificationContext);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return results;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO --
    }

    @Override
    protected RankDoc createRankDocFromHit(int docId, SearchHit hit, int shardRequestIndex) {
        return new RankDocWithSearchHit(docId, hit.getScore(), shardRequestIndex, hit);
    }
}
