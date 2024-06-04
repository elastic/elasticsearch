/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class EmbeddingRequestChunkerTests extends ESTestCase {

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
        assertEquals(batches.get(0).batch().inputs(), inputs);
        var subBatches = batches.get(0).batch().subBatches();
        for (int i = 0; i < inputs.size(); i++) {
            var subBatch = subBatches.get(i);
            assertThat(subBatch.requests(), contains(inputs.get(i)));
            assertEquals(0, subBatch.positions().chunkIndex());
            assertEquals(i, subBatch.positions().inputIndex());
            assertEquals(1, subBatch.positions().embeddingCount());
        }
    }

    public void testManyInputsMakeManyBatches() {
        int maxNumInputsPerBatch = 10;
        int numInputs = maxNumInputsPerBatch * 3 + 1; // requires 4 batches
        var inputs = new ArrayList<String>();
        //
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

        int inputIndex = 0;
        var subBatches = batches.get(0).batch().subBatches();
        for (int i = 0; i < batches.size(); i++) {
            var subBatch = subBatches.get(i);
            assertThat(subBatch.requests(), contains(inputs.get(i)));
            assertEquals(0, subBatch.positions().chunkIndex());
            assertEquals(inputIndex, subBatch.positions().inputIndex());
            assertEquals(1, subBatch.positions().embeddingCount());
            inputIndex++;
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
        {
            var batch = batches.get(0).batch();
            assertThat(batch.inputs(), hasSize(batchSize));
            assertEquals(batchSize, batch.size());
            assertThat(batch.subBatches(), hasSize(2));
            {
                var subBatch = batch.subBatches().get(0);
                assertEquals(0, subBatch.positions().inputIndex());
                assertEquals(0, subBatch.positions().chunkIndex());
                assertEquals(1, subBatch.positions().embeddingCount());
                assertThat(subBatch.requests(), contains("1st small"));
            }
            {
                var subBatch = batch.subBatches().get(1);
                assertEquals(1, subBatch.positions().inputIndex()); // 2nd input
                assertEquals(0, subBatch.positions().chunkIndex());  // 1st part of the 2nd input
                assertEquals(4, subBatch.positions().embeddingCount()); // 4 chunks
                assertThat(subBatch.requests().get(0), startsWith("passage_input0 "));
                assertThat(subBatch.requests().get(1), startsWith(" passage_input20 "));
                assertThat(subBatch.requests().get(2), startsWith(" passage_input40 "));
                assertThat(subBatch.requests().get(3), startsWith(" passage_input60 "));
            }
        }
        {
            var batch = batches.get(1).batch();
            assertThat(batch.inputs(), hasSize(4));
            assertEquals(4, batch.size());
            assertThat(batch.subBatches(), hasSize(3));
            {
                var subBatch = batch.subBatches().get(0);
                assertEquals(1, subBatch.positions().inputIndex()); // 2nd input
                assertEquals(1, subBatch.positions().chunkIndex()); // 2nd part of the 2nd input
                assertEquals(2, subBatch.positions().embeddingCount());
                assertThat(subBatch.requests().get(0), startsWith(" passage_input80 "));
                assertThat(subBatch.requests().get(1), startsWith(" passage_input100 "));
            }
            {
                var subBatch = batch.subBatches().get(1);
                assertEquals(2, subBatch.positions().inputIndex()); // 3rd input
                assertEquals(0, subBatch.positions().chunkIndex());  // 1st and only part
                assertEquals(1, subBatch.positions().embeddingCount()); // 1 chunk
                assertThat(subBatch.requests(), contains("2nd small"));
            }
            {
                var subBatch = batch.subBatches().get(2);
                assertEquals(3, subBatch.positions().inputIndex());  // 4th input
                assertEquals(0, subBatch.positions().chunkIndex());  // 1st and only part
                assertEquals(1, subBatch.positions().embeddingCount()); // 1 chunk
                assertThat(subBatch.requests(), contains("3rd small"));
            }
        }
    }

    public void testMergingListener() {
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

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<TextEmbeddingResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new TextEmbeddingResults.Embedding(new float[] { randomFloat() }));
            }
            batches.get(0).listener().onResponse(new TextEmbeddingResults(embeddings));
        }
        {
            var embeddings = new ArrayList<TextEmbeddingResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new TextEmbeddingResults.Embedding(new float[] { randomFloat() }));
            }
            batches.get(1).listener().onResponse(new TextEmbeddingResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.get(0);
            assertThat(chunkedResult, instanceOf(ChunkedTextEmbeddingFloatResults.class));
            var chunkedFloatResult = (ChunkedTextEmbeddingFloatResults) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals("1st small", chunkedFloatResult.chunks().get(0).matchedText());
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedTextEmbeddingFloatResults.class));
            var chunkedFloatResult = (ChunkedTextEmbeddingFloatResults) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(6));
            assertThat(chunkedFloatResult.chunks().get(0).matchedText(), startsWith("passage_input0 "));
            assertThat(chunkedFloatResult.chunks().get(1).matchedText(), startsWith(" passage_input20 "));
            assertThat(chunkedFloatResult.chunks().get(2).matchedText(), startsWith(" passage_input40 "));
            assertThat(chunkedFloatResult.chunks().get(3).matchedText(), startsWith(" passage_input60 "));
            assertThat(chunkedFloatResult.chunks().get(4).matchedText(), startsWith(" passage_input80 "));
            assertThat(chunkedFloatResult.chunks().get(5).matchedText(), startsWith(" passage_input100 "));
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedTextEmbeddingFloatResults.class));
            var chunkedFloatResult = (ChunkedTextEmbeddingFloatResults) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals("2nd small", chunkedFloatResult.chunks().get(0).matchedText());
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedTextEmbeddingFloatResults.class));
            var chunkedFloatResult = (ChunkedTextEmbeddingFloatResults) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertEquals("3rd small", chunkedFloatResult.chunks().get(0).matchedText());
        }
    }

    public void testListenerErrorsWithWrongNumberOfResponses() {
        List<String> inputs = List.of("1st small", "2nd small", "3rd small");

        var failureMessage = new AtomicReference<String>();
        var listener = new ActionListener<List<ChunkedInferenceServiceResults>>() {

            @Override
            public void onResponse(List<ChunkedInferenceServiceResults> chunkedInferenceServiceResults) {
                assertThat(chunkedInferenceServiceResults.get(0), instanceOf(ErrorChunkedInferenceResults.class));
                var error = (ErrorChunkedInferenceResults) chunkedInferenceServiceResults.get(0);
                failureMessage.set(error.getException().getMessage());
            }

            @Override
            public void onFailure(Exception e) {
                fail("expected a response with an error");
            }
        };

        var batches = new EmbeddingRequestChunker(inputs, 10, 100, 0).batchRequestsWithListeners(listener);
        assertThat(batches, hasSize(1));

        var embeddings = new ArrayList<TextEmbeddingResults.Embedding>();
        embeddings.add(new TextEmbeddingResults.Embedding(new float[] { randomFloat() }));
        embeddings.add(new TextEmbeddingResults.Embedding(new float[] { randomFloat() }));
        batches.get(0).listener().onResponse(new TextEmbeddingResults(embeddings));
        assertEquals("Error the number of embedding responses [2] does not equal the number of requests [3]", failureMessage.get());
    }

    private ChunkedResultsListener testListener() {
        return new ChunkedResultsListener();
    }

    private static class ChunkedResultsListener implements ActionListener<List<ChunkedInferenceServiceResults>> {
        List<ChunkedInferenceServiceResults> results;

        @Override
        public void onResponse(List<ChunkedInferenceServiceResults> chunkedInferenceServiceResults) {
            this.results = chunkedInferenceServiceResults;
        }

        @Override
        public void onFailure(Exception e) {
            fail(e.getMessage());
        }
    }
}
