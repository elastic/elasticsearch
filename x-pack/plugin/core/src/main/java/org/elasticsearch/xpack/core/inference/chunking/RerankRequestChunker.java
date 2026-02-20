/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RerankRequestChunker {
    private final List<String> inputs;
    private final List<RerankChunks> rerankChunks;

    public RerankRequestChunker(String query, List<String> inputs, Integer maxChunksPerDoc) {
        this.inputs = inputs;
        this.rerankChunks = chunk(inputs, buildChunkingSettingsForElasticRerank(query), maxChunksPerDoc);
    }

    private List<RerankChunks> chunk(List<String> inputs, ChunkingSettings chunkingSettings, Integer maxChunksPerDoc) {
        var chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
        var chunks = new ArrayList<RerankChunks>();
        for (int i = 0; i < inputs.size(); i++) {
            var chunksForInput = chunker.chunk(inputs.get(i), chunkingSettings);
            if (maxChunksPerDoc != null && chunksForInput.size() > maxChunksPerDoc) {
                chunksForInput = chunksForInput.subList(0, maxChunksPerDoc);
            }

            for (var chunk : chunksForInput) {
                chunks.add(new RerankChunks(i, inputs.get(i).substring(chunk.start(), chunk.end())));
            }
        }
        return chunks;
    }

    public List<String> getChunkedInputs() {
        List<String> chunkedInputs = new ArrayList<>();
        for (RerankChunks chunk : rerankChunks) {
            chunkedInputs.add(chunk.chunkString());
        }

        return chunkedInputs;
    }

    public ActionListener<InferenceServiceResults> parseChunkedRerankResultsListener(
        ActionListener<InferenceServiceResults> listener,
        boolean returnDocuments
    ) {
        return ActionListener.wrap(results -> {
            if (results instanceof RankedDocsResults rankedDocsResults) {
                listener.onResponse(parseRankedDocResultsForChunks(rankedDocsResults, returnDocuments));

            } else {
                listener.onFailure(new IllegalArgumentException("Expected RankedDocsResults but got: " + results.getClass()));
            }

        }, listener::onFailure);
    }

    private RankedDocsResults parseRankedDocResultsForChunks(RankedDocsResults rankedDocsResults, boolean returnDocuments) {
        List<RankedDocsResults.RankedDoc> topRankedDocs = new ArrayList<>();
        Set<Integer> docIndicesSeen = new HashSet<>();

        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>(rankedDocsResults.getRankedDocs());
        rankedDocs.sort((r1, r2) -> Float.compare(r2.relevanceScore(), r1.relevanceScore()));
        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            int chunkIndex = rankedDoc.index();
            int docIndex = rerankChunks.get(chunkIndex).docIndex();

            if (docIndicesSeen.contains(docIndex) == false) {
                // Create a ranked doc with the full input string and the index for the document instead of the chunk
                RankedDocsResults.RankedDoc updatedRankedDoc = new RankedDocsResults.RankedDoc(
                    docIndex,
                    rankedDoc.relevanceScore(),
                    returnDocuments ? inputs.get(docIndex) : null
                );
                topRankedDocs.add(updatedRankedDoc);
                docIndicesSeen.add(docIndex);
            }
        }

        return new RankedDocsResults(topRankedDocs);
    }

    public record RerankChunks(int docIndex, String chunkString) {};

    private ChunkingSettings buildChunkingSettingsForElasticRerank(String query) {
        var wordIterator = BreakIterator.getWordInstance();
        wordIterator.setText(query);
        var queryWordCount = ChunkerUtils.countWords(0, query.length(), wordIterator);
        return ChunkingSettingsBuilder.buildChunkingSettingsForElasticRerank(queryWordCount);
    }
}
