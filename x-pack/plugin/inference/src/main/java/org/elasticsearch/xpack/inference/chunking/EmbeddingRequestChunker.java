/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.chunking.Chunker.ChunkOffset;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
public class EmbeddingRequestChunker {

    public enum EmbeddingType {
        FLOAT,
        BYTE,
        SPARSE;

        public static EmbeddingType fromDenseVectorElementType(DenseVectorFieldMapper.ElementType elementType) {
            return switch (elementType) {
                case BYTE -> EmbeddingType.BYTE;
                case FLOAT -> EmbeddingType.FLOAT;
                case BIT -> throw new IllegalArgumentException("Bit vectors are not supported");
            };
        }
    };

    // Visible for testing
    record Request(int inputIndex, int chunkIndex, ChunkOffset chunk, List<String> inputs) {
        public String chunkText() {
            return inputs.get(inputIndex).substring(chunk.start(), chunk.end());
        }
    }

    public record BatchRequest(List<Request> requests) {
        public List<String> inputs() {
            return requests.stream().map(Request::chunkText).collect(Collectors.toList());
        }
    }

    public record BatchRequestAndListener(BatchRequest batch, ActionListener<InferenceServiceResults> listener) {}

    private static final int DEFAULT_WORDS_PER_CHUNK = 250;
    private static final int DEFAULT_CHUNK_OVERLAP = 100;

    private final List<String> inputs;
    private final List<List<Request>> requests;
    private final List<BatchRequest> batchRequests;
    private final AtomicInteger resultCount = new AtomicInteger();
    private final EmbeddingType embeddingType;

    private final List<AtomicReferenceArray<EmbeddingResults.Embedding<?>>> results;
    private final AtomicArray<Exception> errors;
    private ActionListener<List<ChunkedInference>> finalListener;

    public EmbeddingRequestChunker(List<String> inputs, int maxNumberOfInputsPerBatch, EmbeddingType embeddingType) {
        this(inputs, maxNumberOfInputsPerBatch, embeddingType, null);
    }

    public EmbeddingRequestChunker(
        List<String> inputs,
        int maxNumberOfInputsPerBatch,
        int wordsPerChunk,
        int chunkOverlap,
        EmbeddingType embeddingType
    ) {
        this(inputs, maxNumberOfInputsPerBatch, embeddingType, new WordBoundaryChunkingSettings(wordsPerChunk, chunkOverlap));
    }

    public EmbeddingRequestChunker(
        List<String> inputs,
        int maxNumberOfInputsPerBatch,
        EmbeddingType embeddingType,
        ChunkingSettings chunkingSettings
    ) {
        this.inputs = inputs;
        this.embeddingType = embeddingType;
        this.results = new ArrayList<>(inputs.size());
        this.errors = new AtomicArray<>(inputs.size());

        if (chunkingSettings == null) {
            chunkingSettings = new WordBoundaryChunkingSettings(DEFAULT_WORDS_PER_CHUNK, DEFAULT_CHUNK_OVERLAP);
        }
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());

        this.requests = new ArrayList<>(inputs.size());

        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            List<ChunkOffset> chunks = chunker.chunk(inputs.get(inputIndex), chunkingSettings);
            List<Request> requestForInput = new ArrayList<>(chunks.size());
            for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                requestForInput.add(new Request(inputIndex, chunkIndex, chunks.get(chunkIndex), inputs));
            }
            requests.add(requestForInput);
            // size the results array with the expected number of request/responses
            results.add(new AtomicReferenceArray<>(chunks.size()));
        }

        AtomicInteger counter = new AtomicInteger();
        this.batchRequests = requests.stream()
            .flatMap(List::stream)
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

        private final BatchRequest request;

        DebatchingListener(BatchRequest request) {
            this.request = request;
        }

        @Override
        public void onResponse(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof EmbeddingResults<?, ?> embeddingResults) {
                if (embeddingResults.embeddings().size() != request.requests.size()) {
                    onFailure(numResultsDoesntMatchException(embeddingResults.embeddings().size(), request.requests.size()));
                    return;
                }
                for (int i = 0; i < embeddingResults.embeddings().size(); i++) {
                    results.get(request.requests().get(i).inputIndex())
                        .set(request.requests().get(i).chunkIndex(), embeddingResults.embeddings().get(i));
                }
                if (resultCount.incrementAndGet() == batchRequests.size()) {
                    sendFinalResponse();
                }
            } else {
                onFailure(unexpectedResultTypeException(inferenceServiceResults.getWriteableName(), TextEmbeddingFloatResults.NAME));
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

        private ElasticsearchStatusException unexpectedResultTypeException(String got, String expected) {
            return new ElasticsearchStatusException(
                "Unexpected inference result type [{}], expected a [{}]",
                RestStatus.INTERNAL_SERVER_ERROR,
                got,
                expected
            );
        }

        @Override
        public void onFailure(Exception e) {
            for (Request request : request.requests) {
                errors.set(request.inputIndex(), e);
            }
            if (resultCount.incrementAndGet() == batchRequests.size()) {
                sendFinalResponse();
            }
        }
    }

    private void sendFinalResponse() {
        var response = new ArrayList<ChunkedInference>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            if (errors.get(i) != null) {
                response.add(new ChunkedInferenceError(errors.get(i)));
            } else {
                response.add(mergeResultsWithInputs(i));
            }
        }
        finalListener.onResponse(response);
    }

    private ChunkedInference mergeResultsWithInputs(int index) {
        List<EmbeddingResults.Chunk> chunks = new ArrayList<>();
        List<Request> request = requests.get(index);
        AtomicReferenceArray<EmbeddingResults.Embedding<?>> result = results.get(index);
        for (int i = 0; i < request.size(); i++) {
            EmbeddingResults.Chunk chunk = result.get(i)
                .toChunk(
                    request.get(i).chunkText(),
                    new ChunkedInference.TextOffset(request.get(i).chunk.start(), request.get(i).chunk.end())
                );
            chunks.add(chunk);
        }
        return new ChunkedInferenceEmbedding(chunks);
    }
}
