/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.common.WordBoundaryChunker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CohereEmbedChunker {

    public static final int MAX_NUM_INPUTS_PER_CALL = 96;
    public static final int WORDS_PER_CHUNK = 300;
    public static final int CHUNK_OVERLAP = 100;

    private final List<BatchRequest> batches = new ArrayList<>();

    private List<List<String>> chunkedInputs;
    private List<AtomicArray<List<TextEmbeddingResults.Embedding>>> results;

    ActionListener<InferenceServiceResults> finalListener;

    private final AtomicInteger resultCount = new AtomicInteger();

    public CohereEmbedChunker(List<String> inputs) {
        splitIntoBatches(inputs);
    }

    private void splitIntoBatches(List<String> inputs) {
        var chunker = new WordBoundaryChunker();

        chunkedInputs = new ArrayList<>(inputs.size());

        results = new ArrayList<>(inputs.size());

        for (int i = 0; i < inputs.size(); i++) {
            var chunks = chunker.chunk(inputs.get(i), WORDS_PER_CHUNK, CHUNK_OVERLAP);
            int numberOfSubBatches = addToBatches(chunks, i);
            results.add(new AtomicArray<>(numberOfSubBatches));
            chunkedInputs.add(chunks);
        }

    }

    int addToBatches(List<String> chunks, int index) {
        var lastBatch = batches.get(batches.size() - 1);

        int freeSpace = MAX_NUM_INPUTS_PER_CALL - lastBatch.size();
        assert freeSpace >= 0;

        int chunkIndex = 0;
        if (freeSpace > 0) {
            lastBatch.addSubBatch(new SubBatch(chunks.subList(0, freeSpace), new SubBatchPositions(index, chunkIndex++)));
        }

        int start = freeSpace;
        while (start < chunks.size()) {
            int toAdd = Math.min(MAX_NUM_INPUTS_PER_CALL, chunks.size() - start);
            var batch = new BatchRequest(
                List.of(new SubBatch(chunks.subList(freeSpace, toAdd), new SubBatchPositions(index, chunkIndex++)))
            );
            batches.add(batch);
            start += toAdd;
        }

        return chunkIndex;
    }


    public record SubBatchPositions(int inputIndex, int chunkIndex) {

    }

    public record SubBatchPositionsAndCount(int count, SubBatchPositions indices) {

    }

    public record SubBatch(List<String> requests, SubBatchPositions positions) {
        public int size() {
            return requests.size();
        }
    }

    public record BatchRequest(List<SubBatch> subBatches) {

        public int size() {
            return subBatches.stream().mapToInt(SubBatch::size).sum();
        }

        public boolean hasFreeCapacity() {
            return size() < MAX_NUM_INPUTS_PER_CALL;
        }

        public void addSubBatch(SubBatch sb) {
            assert size() + sb.size() <= MAX_NUM_INPUTS_PER_CALL;
            subBatches.add(sb);
        }
    }

    public record BatchAndListener(BatchRequest batch, ActionListener<InferenceServiceResults> listener) {

    }

    public BatchAndListener listenerForBatch(BatchRequest batch) {

    }

    private class DebatchingListner implements ActionListener<InferenceServiceResults> {

        private final List<SubBatchPositionsAndCount> positions;

        private final int totalNumberOfRequests;

        DebatchingListner(List<SubBatchPositionsAndCount> positions, int totalNumberOfRequests) {
            this.positions = positions;
            this.totalNumberOfRequests = totalNumberOfRequests;
        }

        @Override
        public void onResponse(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof TextEmbeddingResults textEmbeddingResults) {
                int numRequests = positions.stream().mapToInt(SubBatchPositionsAndCount::count).sum();
                if (numRequests != textEmbeddingResults.embeddings().size()) {
                    onFailure(new ElasticsearchStatusException("What", RestStatus.INTERNAL_SERVER_ERROR));
                    return;
                }

                int start = 0;
                for (var pos : positions) {
                    results.get(pos.indices().inputIndex()).setOnce(pos.indices().chunkIndex(),
                        textEmbeddingResults.embeddings().subList(start, start + pos.count()));
                    start += pos.count();
                }
            }

            if (resultCount.incrementAndGet() == totalNumberOfRequests) {
                sendResponse();
            }
        }

        @Override
        public void onFailure(Exception e) {
//            results.setOnce(index, new ErrorChunkedInferenceResults(e));
            if (resultCount.incrementAndGet() == totalNumberOfRequests) {
                sendResponse();
            }
        }

        private void sendResponse() {

        }

        private List<ChunkedTextEmbeddingResults> merge(List<String> chunks, AtomicArray<List<TextEmbeddingResults.Embedding>> debatchedResults) {
            var all = new ArrayList<TextEmbeddingResults.Embedding>();
            for (int i = 0; i< debatchedResults.length(); i++) {
                var subBatch = debatchedResults.get(i);
                all.addAll(subBatch);
            }

            return ChunkedTextEmbeddingResults.of(chunks, new TextEmbeddingResults(all));
        }


    }
}
