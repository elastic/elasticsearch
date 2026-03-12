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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
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
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class DiversifyRetrieverBuilder extends CompoundRetrieverBuilder<DiversifyRetrieverBuilder> {

    public static final int DEFAULT_SIZE_VALUE = 10;

    public static final NodeFeature RETRIEVER_RESULT_DIVERSIFICATION_MMR_FEATURE = new NodeFeature("retriever.result_diversification_mmr");

    public static final String NAME = "diversify";
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField LAMBDA_FIELD = new ParseField("lambda");
    public static final ParseField SIZE_FIELD = new ParseField("size");

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

    static final ConstructingObjectParser<DiversifyRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            ResultDiversificationType diversificationType = ResultDiversificationType.fromString((String) args[1]);
            String diversificationField = (String) args[2];
            int rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[3];

            VectorData queryVector = args[4] == null ? null : (VectorData) args[4];
            QueryVectorBuilder queryVectorBuilder = args[5] == null ? null : (QueryVectorBuilder) args[5];
            Float lambda = args[6] == null ? null : (Float) args[6];
            Integer size = args[7] == null ? null : (Integer) args[7];

            return new DiversifyRetrieverBuilder(
                RetrieverSource.from((RetrieverBuilder) args[0]),
                diversificationType,
                diversificationField,
                rankWindowSize,
                size,
                queryVector,
                queryVectorBuilder,
                lambda
            );
        }
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (parser, context, n) -> {
            RetrieverBuilder innerRetriever = parser.namedObject(RetrieverBuilder.class, n, context);
            context.trackRetrieverUsage(innerRetriever);
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
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
        PARSER.declareFloat(optionalConstructorArg(), LAMBDA_FIELD);
        PARSER.declareInt(optionalConstructorArg(), SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private final ResultDiversificationType diversificationType;
    private final String diversificationField;
    private final Supplier<VectorData> queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Float lambda;
    private final Integer size;

    DiversifyRetrieverBuilder(
        RetrieverSource innerRetriever,
        ResultDiversificationType diversificationType,
        String diversificationField,
        int rankWindowSize,
        @Nullable Integer size,
        @Nullable VectorData queryVector,
        @Nullable QueryVectorBuilder queryVectorBuilder,
        @Nullable Float lambda
    ) {
        super(List.of(innerRetriever), rankWindowSize);
        this.diversificationType = diversificationType;
        this.diversificationField = diversificationField;
        this.queryVector = queryVector != null ? () -> queryVector : null;
        this.queryVectorBuilder = queryVectorBuilder;
        this.lambda = lambda;
        this.size = size == null ? Math.min(DEFAULT_SIZE_VALUE, rankWindowSize) : size;
    }

    DiversifyRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        ResultDiversificationType diversificationType,
        String diversificationField,
        int rankWindowSize,
        @Nullable Integer size,
        @Nullable Supplier<VectorData> queryVector,
        @Nullable QueryVectorBuilder queryVectorBuilder,
        @Nullable Float lambda
    ) {
        super(innerRetrievers, rankWindowSize);
        assert innerRetrievers.size() == 1 : "ResultDiversificationRetrieverBuilder must have a single child retriever";

        this.diversificationType = diversificationType;
        this.diversificationField = diversificationField;
        this.queryVector = queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.lambda = lambda;
        this.size = size == null ? Math.min(DEFAULT_SIZE_VALUE, rankWindowSize) : size;
    }

    @Override
    protected DiversifyRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return new DiversifyRetrieverBuilder(
            newChildRetrievers,
            diversificationType,
            diversificationField,
            rankWindowSize,
            size,
            queryVector,
            queryVectorBuilder,
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
        if (queryVector != null && queryVectorBuilder != null) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] MMR result diversification can have one of [%s] or [%s], but not both",
                    getName(),
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName()
                ),
                validationException
            );
        }

        if (diversificationType.equals(ResultDiversificationType.MMR)) {
            validationException = validateMMRDiversification(validationException);
        }

        return validationException;
    }

    private ActionRequestValidationException validateMMRDiversification(ActionRequestValidationException validationException) {
        if (this.size <= 0) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] MMR result diversification [%s] of %d must be greater than zero",
                    getName(),
                    SIZE_FIELD.getPreferredName(),
                    this.size
                ),
                validationException
            );
        }

        if (this.size > this.rankWindowSize) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] MMR result diversification [%s] of %d cannot be greater than the [%s] of %d",
                    getName(),
                    SIZE_FIELD.getPreferredName(),
                    this.size,
                    RANK_WINDOW_SIZE_FIELD.getPreferredName(),
                    this.rankWindowSize
                ),
                validationException
            );
        }

        // ensure we have a lambda between 0.0 and 1.0
        if (lambda == null || lambda < 0.0 || lambda > 1.0) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] MMR result diversification must have a [%s] between 0.0 and 1.0. The value provided was %s",
                    getName(),
                    LAMBDA_FIELD.getPreferredName(),
                    lambda == null ? "null" : lambda.toString()
                ),
                validationException
            );
        }
        return validationException;
    }

    @Override
    protected RetrieverBuilder doRewrite(QueryRewriteContext ctx) {
        if (queryVectorBuilder != null) {
            SetOnce<VectorData> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> {
                queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
                    toSet.set(v == null ? null : new VectorData(v));
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

            return new DiversifyRetrieverBuilder(
                innerRetrievers,
                diversificationType,
                diversificationField,
                rankWindowSize,
                size,
                () -> toSet.get(),
                null,
                lambda
            );
        }

        return this;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        SearchSourceBuilder builder = sourceBuilder.from(0);
        builder.trackScores(true);
        return super.finalizeSourceBuilder(builder).docValueField(diversificationField);
    }

    @Override
    protected Exception processInnerItemFailureException(Exception ex) {
        // since we do not have access to the field types before the search actually executes on the shard,
        // we need to check for an exception when the field data is gotten and if it's disabled
        if (ex instanceof SearchPhaseExecutionException spEx) {
            if (spEx.getCause() instanceof ElasticsearchException iaEx) {
                // I'm not a fan of checking the message, but there is no other indicator we can use.
                if (iaEx.getMessage().startsWith("Fielddata is disabled on")) {
                    return new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Failed to retrieve vectors for field [%s]. Is it a [dense_vector] field?",
                            diversificationField
                        ),
                        ex
                    );
                }
            }
        }
        return ex;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        if (rankResults.isEmpty()) {
            return new RankDoc[0];
        }

        if (rankResults.size() > 1) {
            throw new ElasticsearchStatusException("rank results must have only one result set", RestStatus.BAD_REQUEST);
        }

        ScoreDoc[] scoreDocs = rankResults.getFirst();
        if (scoreDocs == null || scoreDocs.length == 0) {
            // might happen in the case where we have no results
            return new RankDoc[0];
        }

        ResultDiversificationContext diversificationContext = getResultDiversificationContext();

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
                if (fieldValue != null) {
                    extractFieldVectorData(asRankDoc.rank, fieldValue, fieldVectors);
                }
            }
        }

        if (fieldVectors.isEmpty()) {
            throw new ElasticsearchStatusException(
                String.format(
                    Locale.ROOT,
                    "Failed to retrieve vectors for field [%s]. Is it a [dense_vector] field?",
                    diversificationField
                ),
                RestStatus.BAD_REQUEST
            );
        }

        diversificationContext.setFieldVectors(fieldVectors);

        try {
            ResultDiversification<?> diversification = ResultDiversificationFactory.getDiversifier(
                diversificationType,
                diversificationContext
            );

            return diversification.diversify(results);
        } catch (IOException e) {
            throw new ElasticsearchStatusException("Result diversification failed", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private ResultDiversificationContext getResultDiversificationContext() {
        if (diversificationType.equals(ResultDiversificationType.MMR)) {
            return new MMRResultDiversificationContext(diversificationField, lambda, size == null ? DEFAULT_SIZE_VALUE : size, queryVector);
        }

        // should not happen
        throw new IllegalArgumentException("Unknown diversification type [" + diversificationType + "]");
    }

    private void extractFieldVectorData(int docId, Object fieldValue, Map<Integer, VectorData> fieldVectors) {
        switch (fieldValue) {
            case float[] floatArray -> {
                fieldVectors.put(docId, new VectorData(floatArray));
                return;
            }
            case byte[] byteArray -> {
                fieldVectors.put(docId, new VectorData(byteArray));
                return;
            }
            case Float[] boxedFloatArray -> {
                fieldVectors.put(docId, new VectorData(unboxedFloatArray(boxedFloatArray)));
                return;
            }
            case Byte[] boxedByteArray -> {
                fieldVectors.put(docId, new VectorData(unboxedByteArray(boxedByteArray)));
                return;
            }
            default -> {
            }
        }

        // CCS search returns a generic Object[] array, so we must
        // examine the individual element type here.
        if (fieldValue instanceof Object[] objectArray) {
            if (objectArray.length == 0) {
                return;
            }

            if (objectArray[0] instanceof Byte) {
                Byte[] asByteArray = Arrays.stream(objectArray).map(x -> (Byte) x).toArray(Byte[]::new);
                fieldVectors.put(docId, new VectorData(unboxedByteArray(asByteArray)));
                return;
            }

            if (objectArray[0] instanceof Float) {
                Float[] asFloatArray = Arrays.stream(objectArray).map(x -> (Float) x).toArray(Float[]::new);
                fieldVectors.put(docId, new VectorData(unboxedFloatArray(asFloatArray)));
                return;
            }
        }

        throw new ElasticsearchStatusException(
            String.format(Locale.ROOT, "Failed to retrieve vectors for field [%s]. Is it a [dense_vector] field?", diversificationField),
            RestStatus.BAD_REQUEST
        );
    }

    private static float[] unboxedFloatArray(Float[] array) {
        float[] unboxedArray = new float[array.length];
        int bIndex = 0;
        for (Float b : array) {
            unboxedArray[bIndex++] = b;
        }
        return unboxedArray;
    }

    private static byte[] unboxedByteArray(Byte[] array) {
        byte[] unboxedArray = new byte[array.length];
        int bIndex = 0;
        for (Byte b : array) {
            unboxedArray[bIndex++] = b;
        }
        return unboxedArray;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static DiversifyRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        builder.field(TYPE_FIELD.getPreferredName(), diversificationType.value);
        builder.field(FIELD_FIELD.getPreferredName(), diversificationField);
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);

        if (queryVector != null) {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector.get());
        }

        if (queryVectorBuilder != null) {
            builder.field(QUERY_VECTOR_BUILDER_FIELD.getPreferredName(), queryVectorBuilder);
        }

        if (lambda != null) {
            builder.field(LAMBDA_FIELD.getPreferredName(), lambda);
        }

        if (size != null) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }
    }

    @Override
    protected RankDoc createRankDocFromHit(int docId, SearchHit hit, int shardRequestIndex) {
        return new RankDocWithSearchHit(docId, hit.getScore(), shardRequestIndex, hit);
    }

    @Override
    public boolean doEquals(Object o) {
        return super.doEquals(o)
            && (o instanceof DiversifyRetrieverBuilder other)
            && this.diversificationType.equals(other.diversificationType)
            && this.diversificationField.equals(other.diversificationField)
            && Objects.equals(this.lambda, other.lambda)
            && ((queryVector == null && other.queryVector == null)
                || (queryVector != null && other.queryVector != null && Objects.equals(queryVector.get(), other.queryVector.get())))
            && Objects.equals(this.queryVectorBuilder, other.queryVectorBuilder);
    }
}
