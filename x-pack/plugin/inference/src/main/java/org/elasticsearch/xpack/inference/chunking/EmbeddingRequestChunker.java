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
import java.util.function.Function;
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

    public static final int DEFAULT_WORDS_PER_CHUNK = 250;
    public static final int DEFAULT_CHUNK_OVERLAP = 100;

    private final List<BatchRequest> batchedRequests = new ArrayList<>();
    private final AtomicInteger resultCount = new AtomicInteger();
    private final int maxNumberOfInputsPerBatch;
    private final int wordsPerChunk;
    private final int chunkOverlap;
    private final EmbeddingType embeddingType;
    private final ChunkingSettings chunkingSettings;

    private List<ChunkOffsetsAndInput> chunkedOffsets;
    private List<AtomicArray<List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding>>> floatResults;
    private List<AtomicArray<List<InferenceByteEmbedding>>> byteResults;
    private List<AtomicArray<List<SparseEmbeddingResults.Embedding>>> sparseResults;
    private AtomicArray<Exception> errors;
    private ActionListener<List<ChunkedInference>> finalListener;

    public EmbeddingRequestChunker(List<String> inputs, int maxNumberOfInputsPerBatch, EmbeddingType embeddingType) {
        this(inputs, maxNumberOfInputsPerBatch, DEFAULT_WORDS_PER_CHUNK, DEFAULT_CHUNK_OVERLAP, embeddingType);
    }

    public EmbeddingRequestChunker(
        List<String> inputs,
        int maxNumberOfInputsPerBatch,
        int wordsPerChunk,
        int chunkOverlap,
        EmbeddingType embeddingType
    ) {
        this.maxNumberOfInputsPerBatch = maxNumberOfInputsPerBatch;
        this.wordsPerChunk = wordsPerChunk;
        this.chunkOverlap = chunkOverlap;
        this.embeddingType = embeddingType;
        this.chunkingSettings = null;
        splitIntoBatchedRequests(inputs);
    }

    public EmbeddingRequestChunker(
        List<String> inputs,
        int maxNumberOfInputsPerBatch,
        EmbeddingType embeddingType,
        ChunkingSettings chunkingSettings
    ) {
        this.maxNumberOfInputsPerBatch = maxNumberOfInputsPerBatch;
        this.wordsPerChunk = DEFAULT_WORDS_PER_CHUNK; // Can be removed after ChunkingConfigurationFeatureFlag is enabled
        this.chunkOverlap = DEFAULT_CHUNK_OVERLAP; // Can be removed after ChunkingConfigurationFeatureFlag is enabled
        this.embeddingType = embeddingType;
        this.chunkingSettings = chunkingSettings;
        splitIntoBatchedRequests(inputs);
    }

    private void splitIntoBatchedRequests(List<String> inputs) {
        Function<String, List<Chunker.ChunkOffset>> chunkFunction;
        if (chunkingSettings != null) {
            var chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
            chunkFunction = input -> chunker.chunk(input, chunkingSettings);
        } else {
            var chunker = new WordBoundaryChunker();
            chunkFunction = input -> chunker.chunk(input, wordsPerChunk, chunkOverlap);
        }

        chunkedOffsets = new ArrayList<>(inputs.size());
        switch (embeddingType) {
            case FLOAT -> floatResults = new ArrayList<>(inputs.size());
            case BYTE -> byteResults = new ArrayList<>(inputs.size());
            case SPARSE -> sparseResults = new ArrayList<>(inputs.size());
        }
        errors = new AtomicArray<>(inputs.size());

        for (int i = 0; i < inputs.size(); i++) {
            var chunks = chunkFunction.apply(inputs.get(i));
            var offSetsAndInput = new ChunkOffsetsAndInput(chunks, inputs.get(i));
            int numberOfSubBatches = addToBatches(offSetsAndInput, i);
            // size the results array with the expected number of request/responses
            switch (embeddingType) {
                case FLOAT -> floatResults.add(new AtomicArray<>(numberOfSubBatches));
                case BYTE -> byteResults.add(new AtomicArray<>(numberOfSubBatches));
                case SPARSE -> sparseResults.add(new AtomicArray<>(numberOfSubBatches));
            }
            chunkedOffsets.add(offSetsAndInput);
        }
    }

    private int addToBatches(ChunkOffsetsAndInput chunk, int inputIndex) {
        BatchRequest lastBatch;
        if (batchedRequests.isEmpty()) {
            lastBatch = new BatchRequest(new ArrayList<>());
            batchedRequests.add(lastBatch);
        } else {
            lastBatch = batchedRequests.get(batchedRequests.size() - 1);
        }

        int freeSpace = maxNumberOfInputsPerBatch - lastBatch.size();
        assert freeSpace >= 0;

        // chunks may span multiple batches,
        // the chunkIndex keeps them ordered.
        int chunkIndex = 0;

        if (freeSpace > 0) {
            // use any free space in the previous batch before creating new batches
            int toAdd = Math.min(freeSpace, chunk.offsets().size());
            lastBatch.addSubBatch(
                new SubBatch(
                    new ChunkOffsetsAndInput(chunk.offsets().subList(0, toAdd), chunk.input()),
                    new SubBatchPositionsAndCount(inputIndex, chunkIndex++, toAdd)
                )
            );
        }

        int start = freeSpace;
        while (start < chunk.offsets().size()) {
            int toAdd = Math.min(maxNumberOfInputsPerBatch, chunk.offsets().size() - start);
            var batch = new BatchRequest(new ArrayList<>());
            batch.addSubBatch(
                new SubBatch(
                    new ChunkOffsetsAndInput(chunk.offsets().subList(start, start + toAdd), chunk.input()),
                    new SubBatchPositionsAndCount(inputIndex, chunkIndex++, toAdd)
                )
            );
            batchedRequests.add(batch);
            start += toAdd;
        }

        return chunkIndex;
    }

    /**
     * Returns a list of batched inputs and a ActionListener for each batch.
     * @param finalListener The listener to call once all the batches are processed
     * @return Batches and listeners
     */
    public List<BatchRequestAndListener> batchRequestsWithListeners(ActionListener<List<ChunkedInference>> finalListener) {
        this.finalListener = finalListener;

        int numberOfRequests = batchedRequests.size();

        var requests = new ArrayList<BatchRequestAndListener>(numberOfRequests);
        for (var batch : batchedRequests) {
            requests.add(
                new BatchRequestAndListener(
                    batch,
                    new DebatchingListener(
                        batch.subBatches().stream().map(SubBatch::positions).collect(Collectors.toList()),
                        numberOfRequests
                    )
                )
            );
        }

        return requests;
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

        private final List<SubBatchPositionsAndCount> positions;
        private final int totalNumberOfRequests;

        DebatchingListener(List<SubBatchPositionsAndCount> positions, int totalNumberOfRequests) {
            this.positions = positions;
            this.totalNumberOfRequests = totalNumberOfRequests;
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
                if (failIfNumRequestsDoNotMatch(floatEmbeddings.embeddings().size())) {
                    return;
                }

                int start = 0;
                for (var pos : positions) {
                    floatResults.get(pos.inputIndex())
                        .setOnce(pos.chunkIndex(), floatEmbeddings.embeddings().subList(start, start + pos.embeddingCount()));
                    start += pos.embeddingCount();
                }

                if (resultCount.incrementAndGet() == totalNumberOfRequests) {
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
                if (failIfNumRequestsDoNotMatch(byteEmbeddings.embeddings().size())) {
                    return;
                }

                int start = 0;
                for (var pos : positions) {
                    byteResults.get(pos.inputIndex())
                        .setOnce(pos.chunkIndex(), byteEmbeddings.embeddings().subList(start, start + pos.embeddingCount()));
                    start += pos.embeddingCount();
                }

                if (resultCount.incrementAndGet() == totalNumberOfRequests) {
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
                if (failIfNumRequestsDoNotMatch(sparseEmbeddings.embeddings().size())) {
                    return;
                }

                int start = 0;
                for (var pos : positions) {
                    sparseResults.get(pos.inputIndex())
                        .setOnce(pos.chunkIndex(), sparseEmbeddings.embeddings().subList(start, start + pos.embeddingCount()));
                    start += pos.embeddingCount();
                }

                if (resultCount.incrementAndGet() == totalNumberOfRequests) {
                    sendResponse();
                }
            } else {
                onFailure(
                    unexpectedResultTypeException(inferenceServiceResults.getWriteableName(), InferenceTextEmbeddingByteResults.NAME)
                );
            }
        }

        private boolean failIfNumRequestsDoNotMatch(int numberOfResults) {
            int numberOfRequests = positions.stream().mapToInt(SubBatchPositionsAndCount::embeddingCount).sum();
            if (numberOfRequests != numberOfResults) {
                onFailure(
                    new ElasticsearchStatusException(
                        "Error the number of embedding responses [{}] does not equal the number of " + "requests [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        numberOfResults,
                        numberOfRequests
                    )
                );
                return true;
            }
            return false;
        }

        private ElasticsearchStatusException unexpectedResultTypeException(String got, String expected) {
            return new ElasticsearchStatusException(
                "Unexpected inference result type [" + got + "], expected a [" + expected + "]",
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }

        @Override
        public void onFailure(Exception e) {
            for (var pos : positions) {
                errors.set(pos.inputIndex(), e);
            }

            if (resultCount.incrementAndGet() == totalNumberOfRequests) {
                sendResponse();
            }
        }

        private void sendResponse() {
            var response = new ArrayList<ChunkedInference>(chunkedOffsets.size());
            for (int i = 0; i < chunkedOffsets.size(); i++) {
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
            case FLOAT -> mergeFloatResultsWithInputs(chunkedOffsets.get(resultIndex), floatResults.get(resultIndex));
            case BYTE -> mergeByteResultsWithInputs(chunkedOffsets.get(resultIndex), byteResults.get(resultIndex));
            case SPARSE -> mergeSparseResultsWithInputs(chunkedOffsets.get(resultIndex), sparseResults.get(resultIndex));
        };
    }

    private ChunkedInferenceEmbeddingFloat mergeFloatResultsWithInputs(
        ChunkOffsetsAndInput chunks,
        AtomicArray<List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding>> debatchedResults
    ) {
        var all = new ArrayList<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding>();
        for (int i = 0; i < debatchedResults.length(); i++) {
            var subBatch = debatchedResults.get(i);
            all.addAll(subBatch);
        }

        assert chunks.size() == all.size();

        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk>();
        for (int i = 0; i < chunks.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk(
                    all.get(i).values(),
                    chunks.chunkText(i),
                    new ChunkedInference.TextOffset(chunks.offsets().get(i).start(), chunks.offsets().get(i).end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingFloat(embeddingChunks);
    }

    private ChunkedInferenceEmbeddingByte mergeByteResultsWithInputs(
        ChunkOffsetsAndInput chunks,
        AtomicArray<List<InferenceByteEmbedding>> debatchedResults
    ) {
        var all = new ArrayList<InferenceByteEmbedding>();
        for (int i = 0; i < debatchedResults.length(); i++) {
            var subBatch = debatchedResults.get(i);
            all.addAll(subBatch);
        }

        assert chunks.size() == all.size();

        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk>();
        for (int i = 0; i < chunks.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingByte.ByteEmbeddingChunk(
                    all.get(i).values(),
                    chunks.chunkText(i),
                    new ChunkedInference.TextOffset(chunks.offsets().get(i).start(), chunks.offsets().get(i).end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingByte(embeddingChunks);
    }

    private ChunkedInferenceEmbeddingSparse mergeSparseResultsWithInputs(
        ChunkOffsetsAndInput chunks,
        AtomicArray<List<SparseEmbeddingResults.Embedding>> debatchedResults
    ) {
        var all = new ArrayList<SparseEmbeddingResults.Embedding>();
        for (int i = 0; i < debatchedResults.length(); i++) {
            var subBatch = debatchedResults.get(i);
            all.addAll(subBatch);
        }

        assert chunks.size() == all.size();

        var embeddingChunks = new ArrayList<ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk>();
        for (int i = 0; i < chunks.size(); i++) {
            embeddingChunks.add(
                new ChunkedInferenceEmbeddingSparse.SparseEmbeddingChunk(
                    all.get(i).tokens(),
                    chunks.chunkText(i),
                    new ChunkedInference.TextOffset(chunks.offsets().get(i).start(), chunks.offsets().get(i).end())
                )
            );
        }

        return new ChunkedInferenceEmbeddingSparse(embeddingChunks);
    }

    public record BatchRequest(List<SubBatch> subBatches) {
        public int size() {
            return subBatches.stream().mapToInt(SubBatch::size).sum();
        }

        public void addSubBatch(SubBatch sb) {
            subBatches.add(sb);
        }

        public List<String> inputs() {
            return subBatches.stream().flatMap(s -> s.requests().toChunkText().stream()).collect(Collectors.toList());
        }
    }

    public record BatchRequestAndListener(BatchRequest batch, ActionListener<InferenceServiceResults> listener) {

    }

    /**
     * Used for mapping batched requests back to the original input
     */
    record SubBatchPositionsAndCount(int inputIndex, int chunkIndex, int embeddingCount) {}

    record SubBatch(ChunkOffsetsAndInput requests, SubBatchPositionsAndCount positions) {
        int size() {
            return requests.offsets().size();
        }
    }

    record ChunkOffsetsAndInput(List<Chunker.ChunkOffset> offsets, String input) {
        List<String> toChunkText() {
            return offsets.stream().map(o -> input.substring(o.start(), o.end())).collect(Collectors.toList());
        }

        int size() {
            return offsets.size();
        }

        String chunkText(int index) {
            return input.substring(offsets.get(index).start(), offsets.get(index).end());
        }
    }
}
