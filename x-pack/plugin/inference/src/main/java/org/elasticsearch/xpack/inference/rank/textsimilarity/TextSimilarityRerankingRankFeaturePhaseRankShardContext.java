/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.inference.chunking.Chunker;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerBuilder;
import org.elasticsearch.xpack.inference.common.chunks.SemanticChunkScorer;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.rank.textsimilarity.ChunkScorerConfig.DEFAULT_SIZE;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.ChunkScorerConfig.defaultChunkingSettings;

public class TextSimilarityRerankingRankFeaturePhaseRankShardContext extends RerankingRankFeaturePhaseRankShardContext {

    public static final FeatureFlag RERANK_SEMANTIC_TEXT_CHUNKS_FEATURE_FLAG = new FeatureFlag("rerank_semantic_text_chunks");
    public static final NodeFeature RERANK_SEMANTIC_TEXT_CHUNKS = new NodeFeature("rerank_semantic_text_chunks");

    private final ChunkScorerConfig chunkScorerConfig;
    private final ChunkingSettings chunkingSettings;

    public TextSimilarityRerankingRankFeaturePhaseRankShardContext(String field, @Nullable ChunkScorerConfig chunkScorerConfig) {
        super(field);
        this.chunkScorerConfig = chunkScorerConfig;
        chunkingSettings = chunkScorerConfig != null ? chunkScorerConfig.chunkingSettings() : null;
    }

    @Override
    public RankShardResult doBuildRankFeatureShardResult(SearchHits hits, int shardId, SearchContext searchContext) {
        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
        for (int i = 0; i < hits.getHits().length; i++) {
            rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
            SearchHit hit = hits.getHits()[i];
            DocumentField docField = hit.field(field);
            if (docField != null) {
                if (chunkScorerConfig != null) {
                    int size = chunkScorerConfig.size() != null ? chunkScorerConfig.size() : DEFAULT_SIZE;

                    SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
                    Mapper mapper = searchExecutionContext.getMappingLookup().getMapper(field);
                    boolean isSemanticTextField = mapper instanceof SemanticTextFieldMapper;

                    List<ScoredChunk> scoredChunks;

                    if (RERANK_SEMANTIC_TEXT_CHUNKS_FEATURE_FLAG.isEnabled() && isSemanticTextField) {
                        SemanticTextFieldMapper semanticTextFieldMapper = (SemanticTextFieldMapper) mapper;
                        SemanticTextFieldMapper.SemanticTextFieldType fieldType = semanticTextFieldMapper.fieldType();

                        // We can't guarantee that all semantic_text embeddings will be compatible with indexed chunking settings,
                        // so we take a hard line, scoring using BM25 if reranking on a semantic_text field with specified chunking_settings
                        if (chunkScorerConfig.chunkingSettings() != null) {
                            scoredChunks = chunkAndScoreBm25(docField.getValue().toString(), size);
                        } else {
                            scoredChunks = scoreSemanticTextChunks(searchContext, fieldType, hit, size);
                        }
                    } else {
                        scoredChunks = chunkAndScoreBm25(docField.getValue().toString(), size);
                    }

                    List<String> bestChunks = scoredChunks.stream().map(ScoredChunk::content).toList();
                    rankFeatureDocs[i].featureData(bestChunks);

                } else {
                    rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
                }
            }
        }
        return new RankFeatureShardResult(rankFeatureDocs);
    }

    private List<ScoredChunk> chunkAndScoreBm25(String value, int size) {
        ChunkingSettings chunkingSettingsOrDefault = chunkingSettings != null ? chunkingSettings : defaultChunkingSettings();
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettingsOrDefault.getChunkingStrategy());
        List<Chunker.ChunkOffset> chunkOffsets = chunker.chunk(value, chunkingSettingsOrDefault);
        List<String> chunks = chunkOffsets.stream().map(offset -> { return value.substring(offset.start(), offset.end()); }).toList();

        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
        try {
            return scorer.scoreChunks(chunks, chunkScorerConfig.inferenceText(), size);
        } catch (IOException e) {
            throw new IllegalStateException("Could not generate chunks for input to reranker", e);
        }
    }

    private List<ScoredChunk> scoreSemanticTextChunks(
        SearchContext searchContext,
        SemanticTextFieldMapper.SemanticTextFieldType fieldType,
        SearchHit hit,
        int size
    ) {
        SemanticChunkScorer scorer = new SemanticChunkScorer(searchContext);
        try {
            List<ScoredChunk> scoredChunks = scorer.scoreChunks(fieldType, hit, chunkScorerConfig.inferenceText(),
                size);
            if (scoredChunks.isEmpty()) {
                scoredChunks = chunkAndScoreBm25(hit.field(field).getValue().toString(), size);
            }
            return scoredChunks;
        } catch (IOException e) {
            throw new IllegalStateException("Could not score semantic text chunks", e);
        }
    }
}
