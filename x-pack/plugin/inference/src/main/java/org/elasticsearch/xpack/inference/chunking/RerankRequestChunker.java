/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RerankRequestChunker {

    private final ChunkingSettings chunkingSettings;
    private final List<String> inputs;
    private final Map<Integer, RerankChunks> rerankChunks;

    public RerankRequestChunker(List<String> inputs) {
        // TODO: Make chunking settings dependent on the model being used.
        // There may be a way to do this dynamically knowing the max token size for the model/service and query size
        // instead of hardcoding it ona model/service basis.
        this.chunkingSettings = new WordBoundaryChunkingSettings(100, 10);
        this.inputs = inputs;
        this.rerankChunks = chunk(inputs, chunkingSettings);
    }

    private Map<Integer, RerankChunks> chunk(List<String> inputs, ChunkingSettings chunkingSettings) {
        var chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
        var chunks = new HashMap<Integer, RerankChunks>();
        var chunkIndex = 0;
        for (int i = 0; i < inputs.size(); i++) {
            var chunksForInput = chunker.chunk(inputs.get(i), chunkingSettings);
            for (var chunk : chunksForInput) {
                chunks.put(chunkIndex, new RerankChunks(i, inputs.get(i).substring(chunk.start(), chunk.end())));
                chunkIndex++;
            }
        }
        return chunks;
    }

    public List<String> getChunkedInputs() {
        List<String> chunkedInputs = new ArrayList<>();
        for (RerankChunks chunk : rerankChunks.values()) {
            chunkedInputs.add(chunk.chunkString());
        }
        // TODO: Score the inputs here and only return the top N chunks for each document
        return chunkedInputs;
    }

    public ActionListener<InferenceAction.Response> parseChunkedRerankResultsListener(ActionListener<InferenceAction.Response> listener) {
        return ActionListener.wrap(results -> {
            if (results.getResults() instanceof RankedDocsResults rankedDocsResults) {
                listener.onResponse(new InferenceAction.Response(parseRankedDocResultsForChunks(rankedDocsResults)));
                // TODO: Figure out if the above correctly creates the response or if it loses any info

            } else {
                listener.onFailure(new IllegalArgumentException("Expected RankedDocsResults but got: " + results.getClass()));
            }

        }, listener::onFailure);
    }

    private RankedDocsResults parseRankedDocResultsForChunks(RankedDocsResults rankedDocsResults) {
        Map<Integer, RankedDocsResults.RankedDoc> bestRankedDocResultPerDoc = new HashMap<>();
        for (var rankedDoc : rankedDocsResults.getRankedDocs()) {
            int chunkIndex = rankedDoc.index();
            int docIndex = rerankChunks.get(chunkIndex).docIndex();
            if (bestRankedDocResultPerDoc.containsKey(docIndex)) {
                RankedDocsResults.RankedDoc existingDoc = bestRankedDocResultPerDoc.get(docIndex);
                if (rankedDoc.relevanceScore() > existingDoc.relevanceScore()) {
                    bestRankedDocResultPerDoc.put(
                        docIndex,
                        new RankedDocsResults.RankedDoc(docIndex, rankedDoc.relevanceScore(), inputs.get(docIndex))
                    );
                }
            } else {
                bestRankedDocResultPerDoc.put(
                    docIndex,
                    new RankedDocsResults.RankedDoc(docIndex, rankedDoc.relevanceScore(), inputs.get(docIndex))
                );
            }
        }
        var bestRankedDocResultPerDocList = new ArrayList<>(bestRankedDocResultPerDoc.values());
        bestRankedDocResultPerDocList.sort(
            (RankedDocsResults.RankedDoc d1, RankedDocsResults.RankedDoc d2) -> Float.compare(d2.relevanceScore(), d1.relevanceScore())
        );
        return new RankedDocsResults(bestRankedDocResultPerDocList);
    }

    public record RerankChunks(int docIndex, String chunkString) {};
}
