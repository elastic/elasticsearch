/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_RERANK_ID;

/**
 * A {@code RetrieverBuilder} for parsing and constructing a text similarity reranker retriever.
 */
public class TextSimilarityRankRetrieverBuilder extends CompoundRetrieverBuilder<TextSimilarityRankRetrieverBuilder> {

    public static final NodeFeature TEXT_SIMILARITY_RERANKER_ALIAS_HANDLING_FIX = new NodeFeature(
        "text_similarity_reranker_alias_handling_fix"
    );
    public static final NodeFeature TEXT_SIMILARITY_RERANKER_MINSCORE_FIX = new NodeFeature("text_similarity_reranker_minscore_fix");
    public static final NodeFeature TEXT_SIMILARITY_RERANKER_SNIPPETS = new NodeFeature("text_similarity_reranker_snippets");

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField INFERENCE_TEXT_FIELD = new ParseField("inference_text");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField FAILURES_ALLOWED_FIELD = new ParseField("allow_rerank_failures");
    public static final ParseField CHUNK_RESCORER_FIELD = new ParseField("chunk_rescorer");
    public static final ParseField CHUNK_SIZE_FIELD = new ParseField("size");
    public static final ParseField CHUNKING_SETTINGS_FIELD = new ParseField("chunking_settings");

    public static final ConstructingObjectParser<TextSimilarityRankRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(TextSimilarityRankBuilder.NAME, args -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            String inferenceId = args[1] == null ? DEFAULT_RERANK_ID : (String) args[1];
            String inferenceText = (String) args[2];
            String field = (String) args[3];
            int rankWindowSize = args[4] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[4];
            boolean failuresAllowed = args[5] != null && (Boolean) args[5];
            ChunkScorerConfig chunkScorerConfig = (ChunkScorerConfig) args[6];

            return new TextSimilarityRankRetrieverBuilder(
                retrieverBuilder,
                inferenceId,
                inferenceText,
                field,
                rankWindowSize,
                failuresAllowed,
                chunkScorerConfig
            );
        });

    private static final ConstructingObjectParser<ChunkScorerConfig, RetrieverParserContext> CHUNK_SCORER_PARSER =
        new ConstructingObjectParser<>(CHUNK_RESCORER_FIELD.getPreferredName(), true, args -> {
            Integer size = (Integer) args[0];
            @SuppressWarnings("unchecked")
            Map<String, Object> chunkingSettingsMap = (Map<String, Object>) args[1];
            ChunkingSettings chunkingSettings = ChunkScorerConfig.chunkingSettingsFromMap(chunkingSettingsMap);
            return new ChunkScorerConfig(size, chunkingSettings);
        });

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder innerRetriever = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(innerRetriever);
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareString(optionalConstructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(constructorArg(), INFERENCE_TEXT_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), FAILURES_ALLOWED_FIELD);
        PARSER.declareObject(optionalConstructorArg(), CHUNK_SCORER_PARSER, CHUNK_RESCORER_FIELD);
        CHUNK_SCORER_PARSER.declareInt(optionalConstructorArg(), CHUNK_SIZE_FIELD);
        CHUNK_SCORER_PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> p.map(), null, CHUNKING_SETTINGS_FIELD);

        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public static TextSimilarityRankRetrieverBuilder fromXContent(
        XContentParser parser,
        RetrieverParserContext context,
        XPackLicenseState licenceState
    ) throws IOException {
        if (TextSimilarityRankBuilder.TEXT_SIMILARITY_RERANKER_FEATURE.check(licenceState) == false) {
            throw LicenseUtils.newComplianceException(TextSimilarityRankBuilder.NAME);
        }
        return PARSER.apply(parser, context);
    }

    private final String inferenceId;
    private final String inferenceText;
    private final String field;
    private final boolean failuresAllowed;
    private final ChunkScorerConfig chunkScorerConfig;

    public TextSimilarityRankRetrieverBuilder(
        RetrieverBuilder retrieverBuilder,
        String inferenceId,
        String inferenceText,
        String field,
        int rankWindowSize,
        boolean failuresAllowed,
        ChunkScorerConfig chunkScorerConfig
    ) {
        super(List.of(RetrieverSource.from(retrieverBuilder)), rankWindowSize);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.failuresAllowed = failuresAllowed;
        this.chunkScorerConfig = chunkScorerConfig;
    }

    public TextSimilarityRankRetrieverBuilder(
        List<RetrieverSource> retrieverSource,
        String inferenceId,
        String inferenceText,
        String field,
        int rankWindowSize,
        Float minScore,
        boolean failuresAllowed,
        String retrieverName,
        List<QueryBuilder> preFilterQueryBuilders,
        ChunkScorerConfig chunkScorerConfig
    ) {
        super(retrieverSource, rankWindowSize);
        if (retrieverSource.size() != 1) {
            throw new IllegalArgumentException("[" + getName() + "] retriever should have exactly one inner retriever");
        }
        if (chunkScorerConfig != null && chunkScorerConfig.size() != null && chunkScorerConfig.size() < 1) {
            throw new IllegalArgumentException("size must be greater than 0, was: " + chunkScorerConfig.size());
        }
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.minScore = minScore;
        this.failuresAllowed = failuresAllowed;
        this.retrieverName = retrieverName;
        this.preFilterQueryBuilders = preFilterQueryBuilders;
        this.chunkScorerConfig = chunkScorerConfig;
    }

    @Override
    protected TextSimilarityRankRetrieverBuilder clone(
        List<RetrieverSource> newChildRetrievers,
        List<QueryBuilder> newPreFilterQueryBuilders
    ) {
        return new TextSimilarityRankRetrieverBuilder(
            newChildRetrievers,
            inferenceId,
            inferenceText,
            field,
            rankWindowSize,
            minScore,
            failuresAllowed,
            retrieverName,
            newPreFilterQueryBuilders,
            chunkScorerConfig
        );
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean enrichResults) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.getFirst();
        List<TextSimilarityRankDoc> filteredDocs = new ArrayList<>();
        // Filtering by min_score must be done here, after reranking.
        // Applying min_score in the child retriever could prematurely exclude documents that would receive high scores from the reranker.
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            assert scoreDoc.score >= 0;
            if (minScore == null || scoreDoc.score >= minScore) {
                if (enrichResults) {
                    filteredDocs.add(
                        new TextSimilarityRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex, inferenceId, field, chunkScorerConfig)
                    );
                } else {
                    filteredDocs.add(new TextSimilarityRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex));
                }
            }
        }
        return filteredDocs.toArray(new TextSimilarityRankDoc[0]);
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.rankBuilder(
            new TextSimilarityRankBuilder(
                field,
                inferenceId,
                inferenceText,
                rankWindowSize,
                minScore,
                failuresAllowed,
                chunkScorerConfig != null
                    ? new ChunkScorerConfig(chunkScorerConfig.size, inferenceText, chunkScorerConfig.chunkingSettings())
                    : null
            )
        );
        return sourceBuilder;
    }

    @Override
    public Set<String> getExtendedUsageFields() {
        Set<String> extendedFields = new HashSet<>();

        if (chunkScorerConfig != null) {
            extendedFields.add(CHUNK_RESCORER_FIELD.getPreferredName());
        }

        return extendedFields;
    }

    @Override
    public String getName() {
        return TextSimilarityRankBuilder.NAME;
    }

    public String inferenceId() {
        return inferenceId;
    }

    public boolean failuresAllowed() {
        return failuresAllowed;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        builder.field(INFERENCE_TEXT_FIELD.getPreferredName(), inferenceText);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        if (failuresAllowed) {
            builder.field(FAILURES_ALLOWED_FIELD.getPreferredName(), failuresAllowed);
        }
        if (chunkScorerConfig != null) {
            builder.startObject(CHUNK_RESCORER_FIELD.getPreferredName());
            if (chunkScorerConfig.size() != null) {
                builder.field(CHUNK_SIZE_FIELD.getPreferredName(), chunkScorerConfig.size());
            }
            if (chunkScorerConfig.chunkingSettings() != null) {
                builder.field(CHUNKING_SETTINGS_FIELD.getPreferredName(), chunkScorerConfig.chunkingSettings().asMap());
            }
            builder.endObject();
        }
    }

    @Override
    public boolean doEquals(Object other) {
        TextSimilarityRankRetrieverBuilder that = (TextSimilarityRankRetrieverBuilder) other;
        return super.doEquals(other)
            && Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(field, that.field)
            && rankWindowSize == that.rankWindowSize
            && Objects.equals(minScore, that.minScore)
            && failuresAllowed == that.failuresAllowed
            && Objects.equals(chunkScorerConfig, that.chunkScorerConfig);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(inferenceId, inferenceText, field, rankWindowSize, minScore, failuresAllowed, chunkScorerConfig);
    }
}
