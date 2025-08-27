/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xpack.core.common.snippets.SnippetScorer;
import org.elasticsearch.xpack.inference.chunking.Chunker;
import org.elasticsearch.xpack.inference.chunking.ChunkerBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.rank.textsimilarity.SnippetConfig.DEFAULT_NUM_SNIPPETS;

public class TextSimilarityRerankingRankFeaturePhaseRankShardContext extends RerankingRankFeaturePhaseRankShardContext {

    private final SnippetConfig snippetRankInput;
    private final ChunkingSettings chunkingSettings;
    private final Chunker chunker;

    public TextSimilarityRerankingRankFeaturePhaseRankShardContext(String field, @Nullable SnippetConfig snippetRankInput) {
        super(field);
        this.snippetRankInput = snippetRankInput;
        chunkingSettings = snippetRankInput != null ? snippetRankInput.chunkingSettings() : null;
        chunker = chunkingSettings != null ? ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy()) : null;
    }

    @Override
    public RankShardResult doBuildRankFeatureShardResult(SearchHits hits, int shardId) {
        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
        for (int i = 0; i < hits.getHits().length; i++) {
            rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
            SearchHit hit = hits.getHits()[i];
            DocumentField docField = hit.field(field);
            if (docField != null) {
                if (snippetRankInput != null) {
                    int numSnippets = snippetRankInput.numSnippets() != null ? snippetRankInput.numSnippets() : DEFAULT_NUM_SNIPPETS;
                    List<Chunker.ChunkOffset> chunkOffsets = chunker.chunk(docField.getValue().toString(), chunkingSettings);
                    List<String> chunks = chunkOffsets.stream()
                        .map(offset -> { return docField.getValue().toString().substring(offset.start(), offset.end()); })
                        .toList();

                    List<String> bestChunks;
                    try {
                        SnippetScorer scorer = new SnippetScorer();
                        List<SnippetScorer.ScoredSnippet> scoredSnippets = scorer.scoreSnippets(
                            chunks,
                            snippetRankInput.inferenceText(),
                            numSnippets
                        );
                        bestChunks = scoredSnippets.stream().map(SnippetScorer.ScoredSnippet::content).limit(numSnippets).toList();
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not generate snippets for input to reranker", e);
                    }
                    rankFeatureDocs[i].featureData(bestChunks);

                } else {
                    rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
                }
            }
        }
        return new RankFeatureShardResult(rankFeatureDocs);
    }

}
