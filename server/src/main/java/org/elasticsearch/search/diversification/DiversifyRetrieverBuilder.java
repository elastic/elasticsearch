/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.EmbeddingsField;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.search.diversification.ResultDiversification.getVectorComparisonScore;
import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class DiversifyRetrieverBuilder extends CompoundRetrieverBuilder<DiversifyRetrieverBuilder> {

    public static final int DEFAULT_SIZE_VALUE = 10;

    public static final NodeFeature RETRIEVER_RESULT_DIVERSIFICATION_MMR_FEATURE = new NodeFeature("retriever.result_diversification_mmr");
    public static final NodeFeature MMR_NULL_DENSE_VECTOR_FIX = new NodeFeature("retriever.mmr_null_dense_vector_fix");
    private static final VectorSimilarityFunction QUERY_VECTOR_SIMILARITY_FUNCTION = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

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

    public DiversifyRetrieverBuilder(
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

    private DiversifyRetrieverBuilder(
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

        // don't handle string encoded query vectors yet
        if (queryVector != null && queryVector.get() != null && queryVector.get().isStringVector()) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] retriever cannot have a [%s] that is string encoded",
                    getName(),
                    QUERY_VECTOR_FIELD.getPreferredName()
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
        // Diversification only needs each hit's score and the embeddings from the diversification field. The source builder created by
        // the base class already suppresses _source and stored fields, so nothing else has to be turned off here.
        return super.finalizeSourceBuilder(
            sourceBuilder.trackScores(true).fetchEmbeddingsField(new EmbeddingsField(diversificationField, VectorType.DENSE_VECTOR))
        );
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
            VectorData vector = getFieldVectorForSearchHit(asRankDoc, diversificationContext);
            if (vector != null) {
                fieldVectors.put(asRankDoc.rank, vector);
            }
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

    /**
     * Returns the single best dense embedding from the diversification field for this hit, or {@code null} if the field is
     * absent, has no values, or has values that cannot be interpreted as dense vectors.
     */
    private VectorData getFieldVectorForSearchHit(RankDocWithSearchHit doc, ResultDiversificationContext diversificationContext) {
        DocumentField field = doc.hit.getFields().get(diversificationField);
        if (field == null) {
            return null;
        }

        List<VectorData> embeddings = extractDenseEmbeddings(field.getValues());
        if (embeddings.isEmpty()) {
            return null;
        }
        if (embeddings.size() == 1) {
            return embeddings.getFirst();
        }

        // Multiple embeddings: pick the one most similar to the query vector.
        VectorData queryVector = diversificationContext.getQueryVector();
        if (queryVector == null) {
            throw new IllegalArgumentException(
                Strings.format(
                    "[%s] or [%s] must be supplied when diversifying on inference field [%s]",
                    QUERY_VECTOR_FIELD.getPreferredName(),
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    diversificationField
                )
            );
        }

        VectorData bestVector = null;
        float currentHighestScore = Float.NEGATIVE_INFINITY;
        for (VectorData embedding : embeddings) {
            float score = getVectorComparisonScore(QUERY_VECTOR_SIMILARITY_FUNCTION, embedding, queryVector);
            if (score > currentHighestScore) {
                bestVector = embedding;
                currentHighestScore = score;
            }
        }
        return bestVector;
    }

    /**
     * Extracts a list of dense vectors from a {@link DocumentField}'s raw values.
     *
     * <p>Three layouts are handled, dispatched on the type of the first element:
     * <ul>
     *   <li><em>Flat scalar list</em> ({@code List<Number>} — {@code dense_vector} source shape): treated as a single
     *       vector and returned as a singleton list. Throws if any element is not a {@link Number}.</li>
     *   <li><em>List of {@code float[]} vectors</em> (one entry per chunk for chunked {@code semantic_text} fields):
     *       each element is converted to a {@link VectorData} independently.</li>
     *   <li><em>Sparse vector map</em> ({@code Map<String, Float>} — token-to-weight pairs from a
     *       {@code sparse_vector} or {@code sparse_embedding} inference field): throws an {@link IllegalArgumentException}, since sparse
     *       vectors are not supported by diversification. Any other {@link Map} shape is returned as an empty list.</li>
     * </ul>
     * Returns an empty list when the values are absent, null, or of an unrecognized type.
     *
     * @throws IllegalArgumentException if the field contains sparse vectors, or malformed dense vectors
     */
    private List<VectorData> extractDenseEmbeddings(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }

        return switch (values.getFirst()) {
            case Number ignored ->
                // Flat scalar list — the entire values list is one vector (the dense_vector source shape).
                parseDenseVectorValue(values);
            case float[] ignored ->
                // Each element is a separate dense embedding (e.g. one float[] per chunk for semantic_text).
                parseInferenceFieldValue(values);
            default ->
                // Silently return an empty list for any other value type. This handles the BwC path where an older node serializes the
                // embeddings field request as a plain fields entry (without the embeddings format), and the field values arrive in an
                // unrecognized shape. On nodes that understand the embeddings field contract, SearchService only fetches fields that
                // can produce embeddings, so this branch is never reached in steady-state.
                List.of();
        };
    }

    private List<VectorData> parseDenseVectorValue(List<Object> values) {
        float[] vec = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) instanceof Number n) {
                vec[i] = n.floatValue();
            } else {
                throw new IllegalArgumentException(
                    "Field [" + diversificationField + "] value is not a well-formed dense vector. Is it a [dense_vector] field?"
                );
            }
        }
        return List.of(new VectorData(vec));
    }

    private List<VectorData> parseInferenceFieldValue(List<Object> values) {
        List<VectorData> embeddings = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value instanceof float[] floatArray) {
                embeddings.add(new VectorData(floatArray));
            } else {
                throw new IllegalArgumentException(
                    "Field ["
                        + diversificationField
                        + "] value is not a well-formed list of dense vectors. Is it a [semantic] or [semantic_text] field?"
                );
            }
        }
        return embeddings;
    }
}
