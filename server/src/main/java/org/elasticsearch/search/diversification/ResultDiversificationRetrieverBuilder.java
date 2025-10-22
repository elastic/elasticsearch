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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class ResultDiversificationRetrieverBuilder extends CompoundRetrieverBuilder<ResultDiversificationRetrieverBuilder> {

    public static final Float DEFAULT_LAMBDA_VALUE = 0.7f;

    public static final String NAME = "diversify";
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_FIELD = new ParseField("query_vector");
    public static final ParseField LAMBDA_FIELD = new ParseField("lambda");

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

            ResultDiversificationType diversificationType = ResultDiversificationType.fromString((String) args[1]);
            String diversificationField = (String) args[2];
            int rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[3];

            @SuppressWarnings("unchecked")
            List<Float> queryVectorList = args[4] == null ? null : (List<Float>) args[4];
            float[] queryVector = null;
            if (queryVectorList != null) {
                queryVector = new float[queryVectorList.size()];
                for (int i = 0; i < queryVectorList.size(); i++) {
                    queryVector[i] = queryVectorList.get(i);
                }
            }

            Float lambda = args[5] == null ? null : (Float) args[5];

            return new ResultDiversificationRetrieverBuilder(
                RetrieverSource.from((RetrieverBuilder) args[0]),
                diversificationType,
                diversificationField,
                rankWindowSize,
                queryVector,
                lambda
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
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private final ResultDiversificationType diversificationType;
    private final String diversificationField;
    private final float[] queryVector;
    private final Float lambda;
    private ResultDiversificationContext diversificationContext = null;

    ResultDiversificationRetrieverBuilder(
        RetrieverSource innerRetriever,
        ResultDiversificationType diversificationType,
        String diversificationField,
        int rankWindowSize,
        @Nullable float[] queryVector,
        @Nullable Float lambda
    ) {
        super(List.of(innerRetriever), rankWindowSize);
        this.diversificationType = diversificationType;
        this.diversificationField = diversificationField;
        this.queryVector = queryVector;
        this.lambda = lambda;

        if (this.diversificationType == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] diversification type must be set to [%s]", NAME, ResultDiversificationType.MMR.value)
            );
        }
    }

    ResultDiversificationRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        ResultDiversificationType diversificationType,
        String diversificationField,
        int rankWindowSize,
        @Nullable float[] queryVector,
        @Nullable Float lambda
    ) {
        super(innerRetrievers, rankWindowSize);
        assert innerRetrievers.size() == 1 : "ResultDiversificationRetrieverBuilder must have a single child retriever";

        this.diversificationType = diversificationType;
        this.diversificationField = diversificationField;
        this.queryVector = queryVector;
        this.lambda = lambda;

        if (this.diversificationType == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] diversification type must be set to [%s]", NAME, ResultDiversificationType.MMR.value)
            );
        }
    }

    @Override
    protected ResultDiversificationRetrieverBuilder clone(
        List<RetrieverSource> newChildRetrievers,
        List<QueryBuilder> newPreFilterQueryBuilders
    ) {
        return new ResultDiversificationRetrieverBuilder(
            newChildRetrievers,
            diversificationType,
            diversificationField,
            rankWindowSize,
            queryVector,
            lambda
        );
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        if (diversificationType.equals(ResultDiversificationType.MMR)) {
            validationException = validateMMRDiversification(validationException);
        }

        return validationException;
    }

    private ActionRequestValidationException validateMMRDiversification(ActionRequestValidationException validationException) {
        // if MMR, ensure we have a lambda between 0.0 and 1.0
        if (lambda == null || lambda < 0.0 || lambda > 1.0) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] MMR result diversification must have a [%s] between 0.0 and 1.0",
                    getName(),
                    LAMBDA_FIELD.getPreferredName()
                ),
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
            throw new IllegalArgumentException(
                "[" + diversificationField + "] is not a supported field type. Valid field types are [dense_vector]"
            );
        }

        if (diversificationType.equals(ResultDiversificationType.MMR)) {
            diversificationContext = new MMRResultDiversificationContext(
                diversificationField,
                lambda == null ? DEFAULT_LAMBDA_VALUE : lambda,
                rankWindowSize,
                (DenseVectorFieldMapper) mapper,
                indexVersion,
                queryVector == null ? null : new VectorData(queryVector),
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
        if (rankResults.isEmpty()) {
            return new RankDoc[0];
        }

        if (rankResults.size() > 1) {
            throw new IllegalArgumentException("rank results must have only one result set");
        }

        ScoreDoc[] scoreDocs = rankResults.getFirst();
        if (scoreDocs == null || scoreDocs.length == 0 || diversificationContext == null) {
            // might happen in the case where we have no results or the context is not set
            return new RankDoc[0];
        }

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
            ResultDiversification<?> diversification = ResultDiversificationFactory.getDiversifier(
                diversificationType,
                diversificationContext
            );
            results = diversification.diversify(results);
        } catch (IOException e) {
            throw new ElasticsearchException("Result diversification failed", e);
        }

        return results;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static ResultDiversificationRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        return PARSER.apply(parser, context);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        builder.field(TYPE_FIELD.getPreferredName(), diversificationType.value);
        builder.field(FIELD_FIELD.getPreferredName(), diversificationField);
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);

        if (queryVector != null) {
            builder.array(QUERY_FIELD.getPreferredName(), queryVector);
        }

        if (lambda != null) {
            builder.field(LAMBDA_FIELD.getPreferredName(), lambda);
        }
    }

    @Override
    protected RankDoc createRankDocFromHit(int docId, SearchHit hit, int shardRequestIndex) {
        return new RankDocWithSearchHit(docId, hit.getScore(), shardRequestIndex, hit);
    }

    @Override
    public boolean doEquals(Object o) {
        return super.doEquals(o)
            && (o instanceof ResultDiversificationRetrieverBuilder other)
            && this.diversificationType.equals(other.diversificationType)
            && this.diversificationField.equals(other.diversificationField)
            && Objects.equals(this.lambda, other.lambda)
            && Arrays.equals(this.queryVector, other.queryVector);
    }
}
