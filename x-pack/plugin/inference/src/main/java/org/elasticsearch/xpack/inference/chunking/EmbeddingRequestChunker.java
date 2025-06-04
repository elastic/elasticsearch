/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.inference.chunking.Chunker.ChunkOffset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class creates optimally sized batches of input strings
 * for batched processing splitting long strings into smaller
 * chunks. Multiple inputs may be fit into a single batch or
 * a single large input that has been chunked may spread over
 * multiple batches.
 *
 * The final aspect it to gather the responses from the batch
 * processing and map the results back to the original element
 * in the input list.
 */
public class EmbeddingRequestChunker<E extends EmbeddingResults.Embedding<E>> {

    // Visible for testing
    record Request(int inputIndex, int chunkIndex, ChunkOffset chunk, List<ChunkInferenceInput> inputs) {
        public String chunkText() {
            return inputs.get(inputIndex).input().substring(chunk.start(), chunk.end());
        }
    }

    public record BatchRequest(List<Request> requests) {
        public Supplier<List<String>> inputs() {
            return () -> requests.stream().map(Request::chunkText).collect(Collectors.toList());
        }
    }

    public record BatchRequestAndListener(BatchRequest batch, ActionListener<InferenceServiceResults> listener) {}

    private static final ChunkingSettings DEFAULT_CHUNKING_SETTINGS = new WordBoundaryChunkingSettings(250, 100);

    // The maximum number of chunks that is stored for any input text.
    // If the configured chunker chunks the text into more chunks, each
    // chunk is sent to the inference service separately, but the results
    // are merged so that only this maximum number of chunks is stored.
    private static final int MAX_CHUNKS = 512;

    private final List<BatchRequest> batchRequests;
    private final AtomicInteger resultCount = new AtomicInteger();

    private final List<List<Integer>> resultOffsetStarts;
    private final List<List<Integer>> resultOffsetEnds;
    private final List<AtomicReferenceArray<E>> resultEmbeddings;
    private final AtomicArray<Exception> resultsErrors;
    private ActionListener<List<ChunkedInference>> finalListener;

    public EmbeddingRequestChunker(List<ChunkInferenceInput> inputs, int maxNumberOfInputsPerBatch) {
        this(inputs, maxNumberOfInputsPerBatch, null);
    }

    public EmbeddingRequestChunker(List<ChunkInferenceInput> inputs, int maxNumberOfInputsPerBatch, int wordsPerChunk, int chunkOverlap) {
        this(inputs, maxNumberOfInputsPerBatch, new WordBoundaryChunkingSettings(wordsPerChunk, chunkOverlap));
    }

    public EmbeddingRequestChunker(
        List<ChunkInferenceInput> inputs,
        int maxNumberOfInputsPerBatch,
        ChunkingSettings defaultChunkingSettings
    ) {
        this.resultEmbeddings = new ArrayList<>(inputs.size());
        this.resultOffsetStarts = new ArrayList<>(inputs.size());
        this.resultOffsetEnds = new ArrayList<>(inputs.size());
        this.resultsErrors = new AtomicArray<>(inputs.size());

        if (defaultChunkingSettings == null) {
            defaultChunkingSettings = DEFAULT_CHUNKING_SETTINGS;
        }

        Map<ChunkingStrategy, Chunker> chunkers = inputs.stream()
            .map(ChunkInferenceInput::chunkingSettings)
            .filter(Objects::nonNull)
            .map(ChunkingSettings::getChunkingStrategy)
            .distinct()
            .collect(Collectors.toMap(chunkingStrategy -> chunkingStrategy, ChunkerBuilder::fromChunkingStrategy));
        Chunker defaultChunker = ChunkerBuilder.fromChunkingStrategy(defaultChunkingSettings.getChunkingStrategy());

        List<Request> allRequests = new ArrayList<>();
        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            ChunkingSettings chunkingSettings = inputs.get(inputIndex).chunkingSettings();
            if (chunkingSettings == null) {
                chunkingSettings = defaultChunkingSettings;
            }
            Chunker chunker = chunkers.getOrDefault(chunkingSettings.getChunkingStrategy(), defaultChunker);
            List<ChunkOffset> chunks = chunker.chunk(inputs.get(inputIndex).input(), chunkingSettings);
            int resultCount = Math.min(chunks.size(), MAX_CHUNKS);
            resultEmbeddings.add(new AtomicReferenceArray<>(resultCount));
            resultOffsetStarts.add(new ArrayList<>(resultCount));
            resultOffsetEnds.add(new ArrayList<>(resultCount));

            for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                // If the number of chunks is larger than the maximum allowed value,
                // scale the indices to [0, MAX) with similar number of original
                // chunks in the final chunks.
                int targetChunkIndex = chunks.size() <= MAX_CHUNKS ? chunkIndex : chunkIndex * MAX_CHUNKS / chunks.size();
                if (resultOffsetStarts.getLast().size() <= targetChunkIndex) {
                    resultOffsetStarts.getLast().add(chunks.get(chunkIndex).start());
                    resultOffsetEnds.getLast().add(chunks.get(chunkIndex).end());
                } else {
                    resultOffsetEnds.getLast().set(targetChunkIndex, chunks.get(chunkIndex).end());
                }
                allRequests.add(new Request(inputIndex, targetChunkIndex, chunks.get(chunkIndex), inputs));
            }
        }

        AtomicInteger counter = new AtomicInteger();
        this.batchRequests = allRequests.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / maxNumberOfInputsPerBatch))
            .values()
            .stream()
            .map(BatchRequest::new)
            .toList();
    }

    /**
     * Returns a list of batched inputs and a ActionListener for each batch.
     * @param finalListener The listener to call once all the batches are processed
     * @return Batches and listeners
     */
    public List<BatchRequestAndListener> batchRequestsWithListeners(ActionListener<List<ChunkedInference>> finalListener) {
        this.finalListener = finalListener;
        return batchRequests.stream().map(req -> new BatchRequestAndListener(req, new DebatchingListener(req))).toList();
    }

    /**
     * A grouping listener that calls the final listener only when
     * all responses have been received.
     * Long inputs that were split into chunks are reassembled and
     * returned as a single chunked response.
     * The listener knows where in the results array to insert the
     * response so that order is preserved.
     */
    private class DebatchingListener implements ActionListener<InferenceServiceResults> {

        private BatchRequest request;

        DebatchingListener(BatchRequest request) {
            this.request = request;
        }

        @Override
        public void onResponse(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof EmbeddingResults<?> == false) {
                onFailure(unexpectedResultTypeException(inferenceServiceResults.getWriteableName()));
                return;
            }
            @SuppressWarnings("unchecked")
            EmbeddingResults<E> embeddingResults = (EmbeddingResults<E>) inferenceServiceResults;
            if (embeddingResults.embeddings().size() != request.requests.size()) {
                onFailure(numResultsDoesntMatchException(embeddingResults.embeddings().size(), request.requests.size()));
                return;
            }
            for (int i = 0; i < embeddingResults.embeddings().size(); i++) {
                E newEmbedding = embeddingResults.embeddings().get(i);
                resultEmbeddings.get(request.requests().get(i).inputIndex())
                    .updateAndGet(
                        request.requests().get(i).chunkIndex(),
                        oldEmbedding -> oldEmbedding == null ? newEmbedding : oldEmbedding.merge(newEmbedding)
                    );
            }
            request = null;
            if (resultCount.incrementAndGet() == batchRequests.size()) {
                sendFinalResponse();
            }
        }

        private ElasticsearchStatusException numResultsDoesntMatchException(int numResults, int numRequests) {
            return new ElasticsearchStatusException(
                "Error the number of embedding responses [{}] does not equal the number of requests [{}]",
                RestStatus.INTERNAL_SERVER_ERROR,
                numResults,
                numRequests
            );
        }

        private ElasticsearchStatusException unexpectedResultTypeException(String resultType) {
            return new ElasticsearchStatusException(
                "Unexpected inference result type [{}], expected [EmbeddingResults]",
                RestStatus.INTERNAL_SERVER_ERROR,
                resultType
            );
        }

        @Override
        public void onFailure(Exception e) {
            for (Request request : request.requests) {
                resultsErrors.set(request.inputIndex(), e);
            }
            this.request = null;
            if (resultCount.incrementAndGet() == batchRequests.size()) {
                sendFinalResponse();
            }
        }
    }

    private void sendFinalResponse() {
        var response = new ArrayList<ChunkedInference>(resultEmbeddings.size());
        for (int i = 0; i < resultEmbeddings.size(); i++) {
            if (resultsErrors.get(i) != null) {
                response.add(new ChunkedInferenceError(resultsErrors.get(i)));
                resultsErrors.set(i, null);
            } else {
                response.add(mergeResultsWithInputs(i));
            }
        }
        finalListener.onResponse(response);
    }

    private ChunkedInference mergeResultsWithInputs(int inputIndex) {
        List<Integer> startOffsets = resultOffsetStarts.get(inputIndex);
        List<Integer> endOffsets = resultOffsetEnds.get(inputIndex);
        AtomicReferenceArray<E> embeddings = resultEmbeddings.get(inputIndex);

        List<EmbeddingResults.Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < embeddings.length(); i++) {
            ChunkedInference.TextOffset offset = new ChunkedInference.TextOffset(startOffsets.get(i), endOffsets.get(i));
            chunks.add(new EmbeddingResults.Chunk(embeddings.get(i), offset));
        }
        return new ChunkedInferenceEmbedding(chunks);
    }
}
