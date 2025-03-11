/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class EmbeddingRequestChunkerTests extends ESTestCase {

    public void testEmptyInput_WordChunker() {
        var batches = new EmbeddingRequestChunker(List.of(), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, empty());
    }

    public void testEmptyInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker(List.of(), 10, new SentenceBoundaryChunkingSettings(250, 1)).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, empty());
    }

    public void testWhitespaceInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker(List.of("   "), 10, new SentenceBoundaryChunkingSettings(250, 1))
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), hasSize(1));
        assertThat(batches.get(0).batch().inputs().get(0), Matchers.is("   "));
    }

    public void testBlankInput_WordChunker() {
        var batches = new EmbeddingRequestChunker(List.of(""), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), hasSize(1));
        assertThat(batches.get(0).batch().inputs().get(0), Matchers.is(""));
    }

    public void testBlankInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker(List.of(""), 10, new SentenceBoundaryChunkingSettings(250, 1)).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), hasSize(1));
        assertThat(batches.get(0).batch().inputs().get(0), Matchers.is(""));
    }

    public void testInputThatDoesNotChunk_WordChunker() {
        var batches = new EmbeddingRequestChunker(List.of("ABBAABBA"), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), hasSize(1));
        assertThat(batches.get(0).batch().inputs().get(0), Matchers.is("ABBAABBA"));
    }

    public void testInputThatDoesNotChunk_SentenceChunker() {
        var batches = new EmbeddingRequestChunker(List.of("ABBAABBA"), 10, new SentenceBoundaryChunkingSettings(250, 1))
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), hasSize(1));
        assertThat(batches.get(0).batch().inputs().get(0), Matchers.is("ABBAABBA"));
    }

    public void testShortInputsAreSingleBatch() {
        String input = "one chunk";
        var batches = new EmbeddingRequestChunker(List.of(input), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.get(0).batch().inputs(), contains(input));
    }

    public void testMultipleShortInputsAreSingleBatch() {
        List<String> inputs = List.of("1st small", "2nd small", "3rd small");
        var batches = new EmbeddingRequestChunker(inputs, 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        EmbeddingRequestChunker.BatchRequest batch = batches.getFirst().batch();
        assertEquals(batch.inputs(), inputs);
        for (int i = 0; i < inputs.size(); i++) {
            var request = batch.requests().get(i);
            assertThat(request.chunkText(), equalTo(inputs.get(i)));
            assertEquals(i, request.inputIndex());
            assertEquals(0, request.chunkIndex());
        }
    }

    public void testManyInputsMakeManyBatches() {
        int maxNumInputsPerBatch = 10;
        int numInputs = maxNumInputsPerBatch * 3 + 1; // requires 4 batches
        var inputs = new ArrayList<String>();

        for (int i = 0; i < numInputs; i++) {
            inputs.add("input " + i);
        }

        var batches = new EmbeddingRequestChunker(inputs, maxNumInputsPerBatch, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(4));
        assertThat(batches.get(0).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(1).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(2).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(3).batch().inputs(), hasSize(1));

        assertEquals("input 0", batches.get(0).batch().inputs().get(0));
        assertEquals("input 9", batches.get(0).batch().inputs().get(9));
        assertThat(
            batches.get(1).batch().inputs(),
            contains("input 10", "input 11", "input 12", "input 13", "input 14", "input 15", "input 16", "input 17", "input 18", "input 19")
        );
        assertEquals("input 20", batches.get(2).batch().inputs().get(0));
        assertEquals("input 29", batches.get(2).batch().inputs().get(9));
        assertThat(batches.get(3).batch().inputs(), contains("input 30"));

        List<EmbeddingRequestChunker.Request> requests = batches.get(0).batch().requests();
        for (int i = 0; i < requests.size(); i++) {
            EmbeddingRequestChunker.Request request = requests.get(i);
            assertThat(request.chunkText(), equalTo(inputs.get(i)));
            assertThat(request.inputIndex(), equalTo(i));
            assertThat(request.chunkIndex(), equalTo(0));
        }
    }

    public void testChunkingSettingsProvided() {
        int maxNumInputsPerBatch = 10;
        int numInputs = maxNumInputsPerBatch * 3 + 1; // requires 4 batches
        var inputs = new ArrayList<String>();

        for (int i = 0; i < numInputs; i++) {
            inputs.add("input " + i);
        }

        var batches = new EmbeddingRequestChunker(inputs, maxNumInputsPerBatch, ChunkingSettingsTests.createRandomChunkingSettings())
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(4));
        assertThat(batches.get(0).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(1).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(2).batch().inputs(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(3).batch().inputs(), hasSize(1));

        assertEquals("input 0", batches.get(0).batch().inputs().get(0));
        assertEquals("input 9", batches.get(0).batch().inputs().get(9));
        assertThat(
            batches.get(1).batch().inputs(),
            contains("input 10", "input 11", "input 12", "input 13", "input 14", "input 15", "input 16", "input 17", "input 18", "input 19")
        );
        assertEquals("input 20", batches.get(2).batch().inputs().get(0));
        assertEquals("input 29", batches.get(2).batch().inputs().get(9));
        assertThat(batches.get(3).batch().inputs(), contains("input 30"));

        List<EmbeddingRequestChunker.Request> requests = batches.get(0).batch().requests();
        for (int i = 0; i < requests.size(); i++) {
            EmbeddingRequestChunker.Request request = requests.get(i);
            assertThat(request.chunkText(), equalTo(inputs.get(i)));
            assertThat(request.inputIndex(), equalTo(i));
            assertThat(request.chunkIndex(), equalTo(0));
        }
    }

    public void testLongInputChunkedOverMultipleBatches() {
        int batchSize = 5;
        int chunkSize = 20;
        int overlap = 0;
        // passage will be chunked into batchSize + 1 parts
        // and spread over 2 batch requests
        int numberOfWordsInPassage = (chunkSize * batchSize) + 5;

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("passage_input").append(i).append(" "); // chunk on whitespace
        }

        List<String> inputs = List.of("1st small", passageBuilder.toString(), "2nd small", "3rd small");

        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(testListener());

        assertThat(batches, hasSize(2));

        var batch = batches.get(0).batch();
        assertThat(batch.inputs(), hasSize(batchSize));
        assertThat(batch.requests(), hasSize(batchSize));

        EmbeddingRequestChunker.Request request = batch.requests().get(0);
        assertThat(request.inputIndex(), equalTo(0));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText(), equalTo("1st small"));

        for (int requestIndex = 1; requestIndex < 5; requestIndex++) {
            request = batch.requests().get(requestIndex);
            assertThat(request.inputIndex(), equalTo(1));
            int chunkIndex = requestIndex - 1;
            assertThat(request.chunkIndex(), equalTo(chunkIndex));
            assertThat(request.chunkText(), startsWith((chunkIndex == 0 ? "" : " ") + "passage_input" + 20 * chunkIndex));
        }

        batch = batches.get(1).batch();
        assertThat(batch.inputs(), hasSize(4));
        assertThat(batch.requests(), hasSize(4));

        for (int requestIndex = 0; requestIndex < 2; requestIndex++) {
            request = batch.requests().get(requestIndex);
            assertThat(request.inputIndex(), equalTo(1));
            int chunkIndex = requestIndex + 4;
            assertThat(request.chunkIndex(), equalTo(chunkIndex));
            assertThat(request.chunkText(), startsWith(" passage_input" + 20 * chunkIndex));
        }

        request = batch.requests().get(2);
        assertThat(request.inputIndex(), equalTo(2));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText(), equalTo("2nd small"));

        request = batch.requests().get(3);
        assertThat(request.inputIndex(), equalTo(3));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText(), equalTo("3rd small"));
    }

    public void testMergingListener_Float() {
        int batchSize = 5;
        int chunkSize = 20;
        int overlap = 0;
        // passage will be chunked into batchSize + 1 parts
        // and spread over 2 batch requests
        int numberOfWordsInPassage = (chunkSize * batchSize) + 5;

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("passage_input").append(i).append(" "); // chunk on whitespace
        }
        List<String> inputs = List.of("a", passageBuilder.toString(), "bb", "ccc");

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new TextEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
            }
            batches.get(0).listener().onResponse(new TextEmbeddingFloatResults(embeddings));
        }
        {
            var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new TextEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
            }
            batches.get(1).listener().onResponse(new TextEmbeddingFloatResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.get(0);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 1), chunkedFloatResult.chunks().get(0).offset());
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(6));
            assertThat(chunkedFloatResult.chunks().get(0).offset(), equalTo(new ChunkedInference.TextOffset(0, 309)));
            assertThat(chunkedFloatResult.chunks().get(1).offset(), equalTo(new ChunkedInference.TextOffset(309, 629)));
            assertThat(chunkedFloatResult.chunks().get(2).offset(), equalTo(new ChunkedInference.TextOffset(629, 949)));
            assertThat(chunkedFloatResult.chunks().get(3).offset(), equalTo(new ChunkedInference.TextOffset(949, 1269)));
            assertThat(chunkedFloatResult.chunks().get(4).offset(), equalTo(new ChunkedInference.TextOffset(1269, 1589)));
            assertThat(chunkedFloatResult.chunks().get(5).offset(), equalTo(new ChunkedInference.TextOffset(1589, 1675)));
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 2), chunkedFloatResult.chunks().get(0).offset());
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 3), chunkedFloatResult.chunks().get(0).offset());
        }
    }

    public void testMergingListener_Byte() {
        int batchSize = 5;
        int chunkSize = 20;
        int overlap = 0;
        // passage will be chunked into batchSize + 1 parts
        // and spread over 2 batch requests
        int numberOfWordsInPassage = (chunkSize * batchSize) + 5;

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("passage_input").append(i).append(" "); // chunk on whitespace
        }
        List<String> inputs = List.of("a", passageBuilder.toString(), "bb", "ccc");

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<TextEmbeddingByteResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new TextEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(0).listener().onResponse(new TextEmbeddingByteResults(embeddings));
        }
        {
            var embeddings = new ArrayList<TextEmbeddingByteResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new TextEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(1).listener().onResponse(new TextEmbeddingByteResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.get(0);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 1), chunkedByteResult.chunks().get(0).offset());
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(6));
            assertThat(chunkedByteResult.chunks().get(0).offset(), equalTo(new ChunkedInference.TextOffset(0, 309)));
            assertThat(chunkedByteResult.chunks().get(1).offset(), equalTo(new ChunkedInference.TextOffset(309, 629)));
            assertThat(chunkedByteResult.chunks().get(2).offset(), equalTo(new ChunkedInference.TextOffset(629, 949)));
            assertThat(chunkedByteResult.chunks().get(3).offset(), equalTo(new ChunkedInference.TextOffset(949, 1269)));
            assertThat(chunkedByteResult.chunks().get(4).offset(), equalTo(new ChunkedInference.TextOffset(1269, 1589)));
            assertThat(chunkedByteResult.chunks().get(5).offset(), equalTo(new ChunkedInference.TextOffset(1589, 1675)));
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 2), chunkedByteResult.chunks().get(0).offset());
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 3), chunkedByteResult.chunks().get(0).offset());
        }
    }

    public void testMergingListener_Bit() {
        int batchSize = 5;
        int chunkSize = 20;
        int overlap = 0;
        // passage will be chunked into batchSize + 1 parts
        // and spread over 2 batch requests
        int numberOfWordsInPassage = (chunkSize * batchSize) + 5;

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("passage_input").append(i).append(" "); // chunk on whitespace
        }
        List<String> inputs = List.of("a", passageBuilder.toString(), "bb", "ccc");

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<TextEmbeddingByteResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new TextEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(0).listener().onResponse(new TextEmbeddingBitResults(embeddings));
        }
        {
            var embeddings = new ArrayList<TextEmbeddingByteResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new TextEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(1).listener().onResponse(new TextEmbeddingBitResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.get(0);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 1), chunkedByteResult.chunks().get(0).offset());
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(6));
            assertThat(chunkedByteResult.chunks().get(0).offset(), equalTo(new ChunkedInference.TextOffset(0, 309)));
            assertThat(chunkedByteResult.chunks().get(1).offset(), equalTo(new ChunkedInference.TextOffset(309, 629)));
            assertThat(chunkedByteResult.chunks().get(2).offset(), equalTo(new ChunkedInference.TextOffset(629, 949)));
            assertThat(chunkedByteResult.chunks().get(3).offset(), equalTo(new ChunkedInference.TextOffset(949, 1269)));
            assertThat(chunkedByteResult.chunks().get(4).offset(), equalTo(new ChunkedInference.TextOffset(1269, 1589)));
            assertThat(chunkedByteResult.chunks().get(5).offset(), equalTo(new ChunkedInference.TextOffset(1589, 1675)));
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 2), chunkedByteResult.chunks().get(0).offset());
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 3), chunkedByteResult.chunks().get(0).offset());
        }
    }

    public void testMergingListener_Sparse() {
        int batchSize = 4;
        int chunkSize = 10;
        int overlap = 0;
        // passage will be chunked into 2.1 batches
        // and spread over 3 batch requests
        int numberOfWordsInPassage = (chunkSize * batchSize * 2) + 5;

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("passage_input").append(i).append(" "); // chunk on whitespace
        }
        List<String> inputs = List.of("a", "bb", "ccc", passageBuilder.toString());

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(3));

        // 4 inputs in 3 batches
        {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken(randomAlphaOfLength(4), 1.0f)), false));
            }
            batches.get(0).listener().onResponse(new SparseEmbeddingResults(embeddings));
        }
        {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken(randomAlphaOfLength(4), 1.0f)), false));
            }
            batches.get(1).listener().onResponse(new SparseEmbeddingResults(embeddings));
        }
        {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 chunks in the final batch
                embeddings.add(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken(randomAlphaOfLength(4), 1.0f)), false));
            }
            batches.get(2).listener().onResponse(new SparseEmbeddingResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.get(0);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 1), chunkedSparseResult.chunks().get(0).offset());
        }
        {
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 2), chunkedSparseResult.chunks().get(0).offset());
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertEquals(new ChunkedInference.TextOffset(0, 3), chunkedSparseResult.chunks().get(0).offset());
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(9)); // passage is split into 9 chunks, 10 words each
            assertThat(chunkedSparseResult.chunks().get(0).offset(), equalTo(new ChunkedInference.TextOffset(0, 149)));
            assertThat(chunkedSparseResult.chunks().get(1).offset(), equalTo(new ChunkedInference.TextOffset(149, 309)));
            assertThat(chunkedSparseResult.chunks().get(8).offset(), equalTo(new ChunkedInference.TextOffset(1269, 1350)));
        }
    }

    public void testListenerErrorsWithWrongNumberOfResponses() {
        List<String> inputs = List.of("1st small", "2nd small", "3rd small");

        var failureMessage = new AtomicReference<String>();
        var listener = new ActionListener<List<ChunkedInference>>() {

            @Override
            public void onResponse(List<ChunkedInference> chunkedResults) {
                assertThat(chunkedResults.get(0), instanceOf(ChunkedInferenceError.class));
                var error = (ChunkedInferenceError) chunkedResults.get(0);
                failureMessage.set(error.exception().getMessage());
            }

            @Override
            public void onFailure(Exception e) {
                fail("expected a response with an error");
            }
        };

        var batches = new EmbeddingRequestChunker(inputs, 10, 100, 0).batchRequestsWithListeners(listener);
        assertThat(batches, hasSize(1));

        var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>();
        embeddings.add(new TextEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
        embeddings.add(new TextEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
        batches.get(0).listener().onResponse(new TextEmbeddingFloatResults(embeddings));
        assertEquals("Error the number of embedding responses [2] does not equal the number of requests [3]", failureMessage.get());
    }

    private ChunkedResultsListener testListener() {
        return new ChunkedResultsListener();
    }

    private static class ChunkedResultsListener implements ActionListener<List<ChunkedInference>> {
        List<ChunkedInference> results;

        @Override
        public void onResponse(List<ChunkedInference> chunks) {
            this.results = chunks;
        }

        @Override
        public void onFailure(Exception e) {
            fail(e.getMessage());
        }
    }
}
