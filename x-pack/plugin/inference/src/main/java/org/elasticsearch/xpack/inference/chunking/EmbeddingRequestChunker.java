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
import org.elasticsearch.xpack.inference.chunking.Chunker.ChunkOffset;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingByte;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingFloat;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingSparse;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.InferenceByteEmbedding;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

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

    private List<AtomicReferenceArray<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding>> floatResults;
    private List<AtomicReferenceArray<InferenceByteEmbedding>> byteResults;
    private List<AtomicReferenceArray<SparseEmbeddingResults.Embedding>> sparseResults;
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

        switch (embeddingType) {
            case FLOAT -> floatResults = new ArrayList<>(inputs.size());
            case BYTE -> byteResults = new ArrayList<>(inputs.size());
            case SPARSE -> sparseResults = new ArrayList<>(inputs.size());
        }
        this.errors = new AtomicArray<>(inputs.size());

        if (chunkingSettings == null) {
            chunkingSettings = new WordBoundaryChunkingSettings(DEFAULT_WORDS_PER_CHUNK, DEFAULT_CHUNK_OVERLAP);
        }
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());

        this.requests = new ArrayList<>(inputs.size());

        for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            List<Chunker.ChunkOffset> chunks = chunker.chunk(inputs.get(inputIndex), chunkingSettings);
            List<Request> requestForInput = new ArrayList<>(chunks.size());
            for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                requestForInput.add(new Request(inputIndex, chunkIndex, chunks.get(chunkIndex), inputs));
            }
            requests.add(requestForInput);

            // size the results array with the expected number of request/responses
            switch (embeddingType) {
                case FLOAT -> floatResults.add(new AtomicReferenceArray<>(chunks.size()));
                case BYTE -> byteResults.add(new AtomicReferenceArray<>(chunks.size()));
                case SPARSE -> sparseResults.add(new AtomicReferenceArray<>(chunks.size()));
            }
        }

        AtomicInteger counter = new AtomicInteger();
        this.batchRequests = requests.stream().flatMap(List::stream)
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / maxNumberOfInputsPerBatch))
            .values()
            .stream().map(BatchRequest::new)
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
            switch (embeddingType) {
                case FLOAT -> handleFloatResults(inferenceServiceResults);
                case BYTE -> handleByteResults(inferenceServiceResults);
                case SPARSE -> handleSparseResults(inferenceServiceResults);
            }
        }

        private void handleFloatResults(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof InferenceTextEmbeddingFloatResults floatEmbeddings) {
                if (floatEmbeddings.embeddings().size() != request.requests.size()) {
                    onFailure(numResultsDoesntMatchException(floatEmbeddings.embeddings().size(), request.requests.size()));
                    return;
                }
                for (int i = 0; i < floatEmbeddings.embeddings().size(); i++) {
                    floatResults.get(request.requests().get(i).inputIndex())
                        .set(request.requests().get(i).chunkIndex(), floatEmbeddings.embeddings().get(i));
                }

                if (resultCount.incrementAndGet() == batchRequests.size()) {
                    sendResponse();
                }
            } else {
                onFailure(
                    unexpectedResultTypeException(inferenceServiceResults.getWriteableName(), InferenceTextEmbeddingFloatResults.NAME)
                );
            }
        }

        private void handleByteResults(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof InferenceTextEmbeddingByteResults byteEmbeddings) {
                if (byteEmbeddings.embeddings().size() != request.requests.size()) {
                    onFailure(numResultsDoesntMatchException(byteEmbeddings.embeddings().size(), request.requests.size()));
                    return;
                }
                for (int i = 0; i < byteEmbeddings.embeddings().size(); i++) {
                    byteResults.get(request.requests().get(i).inputIndex())
                        .set(request.requests().get(i).chunkIndex(), byteEmbeddings.embeddings().get(i));
                }

                if (resultCount.incrementAndGet() == batchRequests.size()) {
                    sendResponse();
                }
            } else {
                onFailure(
                    unexpectedResultTypeException(inferenceServiceResults.getWriteableName(), InferenceTextEmbeddingByteResults.NAME)
                );
            }
        }

        private void handleSparseResults(InferenceServiceResults inferenceServiceResults) {
            if (inferenceServiceResults instanceof SparseEmbeddingResults sparseEmbeddings) {
                if (sparseEmbeddings.embeddings().size() != request.requests.size()) {
                    onFailure(numResultsDoesntMatchException(sparseEmbeddings.embeddings().size(), request.requests.size()));
                    return;
                }
                for (int i = 0; i < sparseEmbeddings.embeddings().size(); i++) {
                    sparseResults.get(request.requests().get(i).inputIndex())
                        .set(request.requests().get(i).chunkIndex(), sparseEmbeddings.embeddings().get(i));
                }

                if (resultCount.incrementAndGet() == batchRequests.size()) {
                    sendResponse();
                }
            } else {
                onFailure(
                    unexpectedResultTypeException(inferenceServiceResults.getWriteableName(), InferenceTextEmbeddingByteResults.NAME)
                );
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
                sendResponse();
            }
        }

        private void sendResponse() {
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
    }

    private ChunkedInference mergeResultsWithInputs(int resultIndex) {
        return switch (embeddingType) {
            case FLOAT -> mergeFloatResultsWithInputs(requests.get(resultIndex), floatResults.get(resultIndex));
            case BYTE -> mergeByteResultsWithInputs(requests.get(resultIndex), byteResults.get(resultIndex));
            case SPARSE -> mergeSparseResultsWithInputs(requests.get(resultIndex), sparseResults.get(resultIndex));
        };
    }

    private ChunkedInferenceEmbeddingFloat mergeFloatResultsWithInputs(
        List<Request> requests,
        AtomicReferenceArray<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddings
    ) {
        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk>();
        for (int i = 0; i < requests.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk(
                    embeddings.get(i).values(),
                    requests.get(i).chunkText(),
                    new ChunkedInference.TextOffset(requests.get(i).chunk.start(), requests.get(i).chunk.end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingFloat(embeddingChunks);
    }

    private ChunkedInferenceEmbeddingByte mergeByteResultsWithInputs(
        List<Request> requests,
        AtomicReferenceArray<InferenceByteEmbedding> embeddings
    ) {
        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk>();
        for (int i = 0; i < requests.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk(
                    embeddings.get(i).values(),
                    requests.get(i).chunkText(),
                    new ChunkedInference.TextOffset(requests.get(i).chunk.start(), requests.get(i).chunk.end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingByte(embeddingChunks);
    }

    private ChunkedInferenceEmbeddingSparse mergeSparseResultsWithInputs(
        List<Request> requests,
        AtomicReferenceArray<SparseEmbeddingResults.Embedding> embeddings
    ) {
        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk>();
        for (int i = 0; i < requests.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk(
                    embeddings.get(i).tokens(),
                    requests.get(i).chunkText(),
                    new ChunkedInference.TextOffset(requests.get(i).chunk.start(), requests.get(i).chunk.end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingSparse(embeddingChunks);
    }
}
