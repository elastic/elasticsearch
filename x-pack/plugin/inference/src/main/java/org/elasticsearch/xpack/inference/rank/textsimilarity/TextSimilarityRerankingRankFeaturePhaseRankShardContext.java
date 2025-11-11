/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
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

public class TextSimilarityRerankingRankFeaturePhaseRankShardContext extends RerankingRankFeaturePhaseRankShardContext {

    private final ChunkScorerConfig chunkScorerConfig;
    private final ChunkingSettings chunkingSettings;
    private final Chunker chunker;

    public TextSimilarityRerankingRankFeaturePhaseRankShardContext(String field, @Nullable ChunkScorerConfig chunkScorerConfig) {
        super(field);
        this.chunkScorerConfig = chunkScorerConfig;
        chunkingSettings = chunkScorerConfig != null ? chunkScorerConfig.chunkingSettings() : null;
        chunker = chunkingSettings != null ? ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy()) : null;
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

                    MappingLookup mappingLookup = searchContext.indexService().mapperService().mappingLookup();
                    Mapper mapper = mappingLookup.getMapper(field);
                    boolean isSemanticTextField = mapper instanceof SemanticTextFieldMapper;

                    List<ScoredChunk> scoredChunks;

                    if (isSemanticTextField) {
                        SemanticTextFieldMapper semanticTextFieldMapper = (SemanticTextFieldMapper) mapper;
                        SemanticTextFieldMapper.SemanticTextFieldType fieldType = semanticTextFieldMapper.fieldType();

                        if (hasIncompatibleChunkingSettings(fieldType)) {
                            HeaderWarning.addWarning("""
                                Specified chunking settings do not match semantic_text embeddings, returned chunks will be scored using BM25
                                """);
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
        List<Chunker.ChunkOffset> chunkOffsets = chunker.chunk(value, chunkingSettings);
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
            return scorer.scoreChunks(fieldType, hit, chunkScorerConfig.inferenceText(), size);
        } catch (IOException e) {
            throw new IllegalStateException("Could not score semantic text chunks", e);
        }
    }

    private boolean hasIncompatibleChunkingSettings(SemanticTextFieldMapper.SemanticTextFieldType fieldType) {
        //
        // Here, I'm trying to be aggressive about detecting whether we should be able to use existing semantic_text embeddings.
        // If no chunking settings are specified in the retriever request, we assume we are good to go.
        // If we have specified chunking settings, we check them against the existing semantic text chunking settings
        // and if they are equivalent, we're still good to go.
        // This does open up a potential edge case, because chunking settings are updateable and it's possibe that the chunking
        // settings were updated but we still have chunks with an old chunking settings that have not yet been reindexed.
        // Our other option is to take a hard line and always return a warning if chunking settings are specified.
        // I decided to be more lenient here to open up discussion in the POC.
        if (chunkingSettings == null) {
            return false;
        }
        ChunkingSettings semanticTextChunkingSettings = fieldType.getChunkingSettings();
        if (semanticTextChunkingSettings == null) {
            return false;
        }
        return semanticTextChunkingSettings.equals(chunkingSettings) == false;
    }

}
