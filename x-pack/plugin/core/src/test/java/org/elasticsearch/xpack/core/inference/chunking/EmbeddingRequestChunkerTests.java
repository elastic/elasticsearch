/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.inference.InferenceString.DataType.TEXT;
import static org.elasticsearch.inference.InferenceString.toStringList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class EmbeddingRequestChunkerTests extends ESTestCase {

    private static final int MAX_BATCH_SIZE = 512;

    public void testEmptyInput_WordChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, empty());
    }

    public void testEmptyInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(), 10, new SentenceBoundaryChunkingSettings(250, 1)).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, empty());
    }

    public void testEmptyInput_NoopChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(), 10, NoneChunkingSettings.INSTANCE).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, empty());
    }

    public void testAnyInput_NoopChunker() {
        var randomInput = randomAlphaOfLengthBetween(100, 1000);
        var batches = new EmbeddingRequestChunker<>(List.of(new ChunkInferenceInput(randomInput)), 10, NoneChunkingSettings.INSTANCE)
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is(randomInput));
    }

    public void testWhitespaceInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker<>(
            List.of(new ChunkInferenceInput("   ")),
            10,
            new SentenceBoundaryChunkingSettings(250, 1)
        ).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is("   "));
    }

    public void testBlankInput_WordChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(new ChunkInferenceInput("")), 100, 100, 10).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is(""));
    }

    public void testBlankInput_SentenceChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(new ChunkInferenceInput("")), 10, new SentenceBoundaryChunkingSettings(250, 1))
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is(""));
    }

    public void testInputThatDoesNotChunk_WordChunker() {
        var batches = new EmbeddingRequestChunker<>(List.of(new ChunkInferenceInput("ABBAABBA")), 100, 100, 10).batchRequestsWithListeners(
            testListener()
        );
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is("ABBAABBA"));
    }

    public void testInputThatDoesNotChunk_SentenceChunker() {
        var batches = new EmbeddingRequestChunker<>(
            List.of(new ChunkInferenceInput("ABBAABBA")),
            10,
            new SentenceBoundaryChunkingSettings(250, 1)
        ).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get(), hasSize(1));
        assertThat(batches.getFirst().batch().inputs().get().getFirst().value(), is("ABBAABBA"));
    }

    public void testShortInputsAreSingleBatch() {
        ChunkInferenceInput input = new ChunkInferenceInput("one chunk");
        var batches = new EmbeddingRequestChunker<>(List.of(input), 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        assertThat(toStringList(batches.getFirst().batch().inputs().get()), contains(input.inputText()));
    }

    public void testMultipleShortInputsAreSingleBatch() {
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );
        var batches = new EmbeddingRequestChunker<>(inputs, 100, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(1));
        EmbeddingRequestChunker.BatchRequest batch = batches.getFirst().batch();
        assertEquals(batch.inputs().get(), ChunkInferenceInput.inputs(inputs));
        for (int i = 0; i < inputs.size(); i++) {
            var request = batch.requests().get(i);
            assertThat(request.chunkText().value(), equalTo(inputs.get(i).inputText()));
            assertEquals(i, request.inputIndex());
            assertEquals(0, request.chunkIndex());
        }
    }

    public void testManyInputsMakeManyBatches() {
        int maxNumInputsPerBatch = 10;
        int numInputs = maxNumInputsPerBatch * 3 + 1; // requires 4 batches
        var inputs = new ArrayList<ChunkInferenceInput>();

        for (int i = 0; i < numInputs; i++) {
            inputs.add(new ChunkInferenceInput("input " + i));
        }

        var batches = new EmbeddingRequestChunker<>(inputs, maxNumInputsPerBatch, 100, 10).batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(4));
        assertThat(batches.get(0).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(1).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(2).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(3).batch().inputs().get(), hasSize(1));

        assertEquals("input 0", batches.get(0).batch().inputs().get().get(0).value());
        assertEquals("input 9", batches.get(0).batch().inputs().get().get(9).value());
        assertThat(
            toStringList(batches.get(1).batch().inputs().get()),
            contains("input 10", "input 11", "input 12", "input 13", "input 14", "input 15", "input 16", "input 17", "input 18", "input 19")
        );
        assertEquals("input 20", batches.get(2).batch().inputs().get().get(0).value());
        assertEquals("input 29", batches.get(2).batch().inputs().get().get(9).value());
        assertThat(toStringList(batches.get(3).batch().inputs().get()), contains("input 30"));

        List<EmbeddingRequestChunker.Request> requests = batches.get(0).batch().requests();
        for (int i = 0; i < requests.size(); i++) {
            EmbeddingRequestChunker.Request request = requests.get(i);
            assertThat(request.chunkText().value(), equalTo(inputs.get(i).inputText()));
            assertThat(request.inputIndex(), equalTo(i));
            assertThat(request.chunkIndex(), equalTo(0));
        }
    }

    public void testChunkingSettingsProvided() {
        int maxNumInputsPerBatch = 10;
        int numInputs = maxNumInputsPerBatch * 3 + 1; // requires 4 batches
        var inputs = new ArrayList<ChunkInferenceInput>();

        for (int i = 0; i < numInputs; i++) {
            inputs.add(new ChunkInferenceInput("input " + i));
        }

        var batches = new EmbeddingRequestChunker<>(inputs, maxNumInputsPerBatch, ChunkingSettingsTests.createRandomChunkingSettings())
            .batchRequestsWithListeners(testListener());
        assertThat(batches, hasSize(4));
        assertThat(batches.get(0).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(1).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(2).batch().inputs().get(), hasSize(maxNumInputsPerBatch));
        assertThat(batches.get(3).batch().inputs().get(), hasSize(1));

        assertEquals("input 0", batches.get(0).batch().inputs().get().get(0).value());
        assertEquals("input 9", batches.get(0).batch().inputs().get().get(9).value());
        assertThat(
            toStringList(batches.get(1).batch().inputs().get()),
            contains("input 10", "input 11", "input 12", "input 13", "input 14", "input 15", "input 16", "input 17", "input 18", "input 19")
        );
        assertEquals("input 20", batches.get(2).batch().inputs().get().get(0).value());
        assertEquals("input 29", batches.get(2).batch().inputs().get().get(9).value());
        assertThat(toStringList(batches.get(3).batch().inputs().get()), contains("input 30"));

        List<EmbeddingRequestChunker.Request> requests = batches.get(0).batch().requests();
        for (int i = 0; i < requests.size(); i++) {
            EmbeddingRequestChunker.Request request = requests.get(i);
            assertThat(request.chunkText().value(), equalTo(inputs.get(i).inputText()));
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

        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );

        var batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(testListener());

        assertThat(batches, hasSize(2));

        var batch = batches.getFirst().batch();
        assertThat(batch.inputs().get(), hasSize(batchSize));
        assertThat(batch.requests(), hasSize(batchSize));

        EmbeddingRequestChunker.Request request = batch.requests().getFirst();
        assertThat(request.inputIndex(), equalTo(0));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText().value(), equalTo("1st small"));

        for (int requestIndex = 1; requestIndex < 5; requestIndex++) {
            request = batch.requests().get(requestIndex);
            assertThat(request.inputIndex(), equalTo(1));
            int chunkIndex = requestIndex - 1;
            assertThat(request.chunkIndex(), equalTo(chunkIndex));
            assertThat(request.chunkText().value(), startsWith((chunkIndex == 0 ? "" : " ") + "passage_input" + 20 * chunkIndex));
        }

        batch = batches.get(1).batch();
        assertThat(batch.inputs().get(), hasSize(4));
        assertThat(batch.requests(), hasSize(4));

        for (int requestIndex = 0; requestIndex < 2; requestIndex++) {
            request = batch.requests().get(requestIndex);
            assertThat(request.inputIndex(), equalTo(1));
            int chunkIndex = requestIndex + 4;
            assertThat(request.chunkIndex(), equalTo(chunkIndex));
            assertThat(request.chunkText().value(), startsWith(" passage_input" + 20 * chunkIndex));
        }

        request = batch.requests().get(2);
        assertThat(request.inputIndex(), equalTo(2));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText().value(), equalTo("2nd small"));

        request = batch.requests().get(3);
        assertThat(request.inputIndex(), equalTo(3));
        assertThat(request.chunkIndex(), equalTo(0));
        assertThat(request.chunkText().value(), equalTo("3rd small"));
    }

    public void testVeryLongInput_Sparse() {
        int batchSize = 5;
        int chunkSize = 20;
        int numberOfWordsInPassage = (chunkSize * 10000);

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("word").append(i).append(" "); // chunk on whitespace
        }

        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small")
        );

        var finalListener = testListener();
        List<EmbeddingRequestChunker.BatchRequestAndListener> batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, 0)
            .batchRequestsWithListeners(finalListener);

        // The very long passage is split into 10000 chunks for inference, so
        // there are 10002 inference requests, resulting in 2001 batches.
        assertThat(batches, hasSize(2001));
        for (int i = 0; i < 2000; i++) {
            assertThat(batches.get(i).batch().inputs().get(), hasSize(5));
        }
        assertThat(batches.get(2000).batch().inputs().get(), hasSize(2));

        // Produce inference results for each request, with just the token
        // "word" and increasing weights.
        float weight = 0f;
        for (var batch : batches) {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < batch.batch().requests().size(); i++) {
                weight += 1 / 16384f;
                embeddings.add(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken("word", weight)), false));
            }
            batch.listener().onResponse(new SparseEmbeddingResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(3));

        // The first input has the token with weight 1/16384f.
        ChunkedInference inference = finalListener.results.getFirst();
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        ChunkedInferenceEmbedding chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.getFirst().inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("1st small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
        SparseEmbeddingResults.Embedding embedding = (SparseEmbeddingResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.tokens(), contains(new WeightedToken("word", 1 / 16384f)));

        // The very long passage "word0 word1 ... word199999" is split into 10000 chunks for
        // inference. They get the embeddings with token "word" and weights 2/1024 ... 10000/16384.
        // Next, they are merged into 512 larger chunks, which consists of 19 or 20 smaller chunks
        // and therefore 380 or 400 words. For each, the max token weights are collected.
        inference = finalListener.results.get(1);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(512));

        // The first merged chunk consists of 20 small chunks (so 400 words) and the max
        // weight is the weight of the 20th small chunk (so 21/16384).
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), startsWith("word0 word1 "));
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), endsWith(" word398 word399"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
        embedding = (SparseEmbeddingResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.tokens(), contains(new WeightedToken("word", 21 / 16384f)));

        // The last merged chunk consists of 19 small chunks (so 380 words) and the max
        // weight is the weight of the 10000th small chunk (so 10001/16384).
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            startsWith(" word199620 word199621 ")
        );
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            endsWith(" word199998 word199999")
        );
        assertThat(chunkedEmbedding.chunks().get(511).embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
        embedding = (SparseEmbeddingResults.Embedding) chunkedEmbedding.chunks().get(511).embedding();
        assertThat(embedding.tokens(), contains(new WeightedToken("word", 10001 / 16384f)));

        // The last input has the token with weight 10002/16384.
        inference = finalListener.results.get(2);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.get(2).inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("2nd small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
        embedding = (SparseEmbeddingResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.tokens(), contains(new WeightedToken("word", 10002 / 16384f)));
    }

    public void testVeryLongInput_Float() {
        int batchSize = 5;
        int chunkSize = 20;
        int numberOfWordsInPassage = (chunkSize * 10000);

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("word").append(i).append(" "); // chunk on whitespace
        }

        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small")
        );

        var finalListener = testListener();
        List<EmbeddingRequestChunker.BatchRequestAndListener> batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, 0)
            .batchRequestsWithListeners(finalListener);

        // The very long passage is split into 10000 chunks for inference, so
        // there are 10002 inference requests, resulting in 2001 batches.
        assertThat(batches, hasSize(2001));
        for (int i = 0; i < 2000; i++) {
            assertThat(batches.get(i).batch().inputs().get(), hasSize(5));
        }
        assertThat(batches.get(2000).batch().inputs().get(), hasSize(2));

        // Produce inference results for each request, with increasing weights.
        float weight = 0f;
        for (var batch : batches) {
            var embeddings = new ArrayList<DenseEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < batch.batch().requests().size(); i++) {
                weight += 1 / 16384f;
                embeddings.add(new DenseEmbeddingFloatResults.Embedding(new float[] { weight }));
            }
            batch.listener().onResponse(new DenseEmbeddingFloatResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(3));

        // The first input has the embedding with weight 1/16384.
        ChunkedInference inference = finalListener.results.getFirst();
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        ChunkedInferenceEmbedding chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.getFirst().inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("1st small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
        DenseEmbeddingFloatResults.Embedding embedding = (DenseEmbeddingFloatResults.Embedding) chunkedEmbedding.chunks()
            .getFirst()
            .embedding();
        assertThat(embedding.values(), equalTo(new float[] { 1 / 16384f }));

        // The very long passage "word0 word1 ... word199999" is split into 10000 chunks for
        // inference. They get the embeddings with weights 2/1024 ... 10000/16384.
        // Next, they are merged into 512 larger chunks, which consists of 19 or 20 smaller chunks
        // and therefore 380 or 400 words. For each, the average weight is collected.
        inference = finalListener.results.get(1);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(512));

        // The first merged chunk consists of 20 small chunks (so 400 words) and the weight
        // is the average of the weights 2/16384 ... 21/16384.
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), startsWith("word0 word1 "));
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), endsWith(" word398 word399"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
        embedding = (DenseEmbeddingFloatResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.values(), equalTo(new float[] { (2 + 21) / (2 * 16384f) }));

        // The last merged chunk consists of 19 small chunks (so 380 words) and the weight
        // is the average of the weights 9983/16384 ... 10001/16384.
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            startsWith(" word199620 word199621 ")
        );
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            endsWith(" word199998 word199999")
        );
        assertThat(chunkedEmbedding.chunks().get(511).embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
        embedding = (DenseEmbeddingFloatResults.Embedding) chunkedEmbedding.chunks().get(511).embedding();
        assertThat(embedding.values(), equalTo(new float[] { (9983 + 10001) / (2 * 16384f) }));

        // The last input has the token with weight 10002/16384.
        inference = finalListener.results.get(2);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.get(2).inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("2nd small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
        embedding = (DenseEmbeddingFloatResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.values(), equalTo(new float[] { 10002 / 16384f }));
    }

    public void testVeryLongInput_Byte() {
        int batchSize = 5;
        int chunkSize = 20;
        int numberOfWordsInPassage = (chunkSize * 10000);

        var passageBuilder = new StringBuilder();
        for (int i = 0; i < numberOfWordsInPassage; i++) {
            passageBuilder.append("word").append(i).append(" "); // chunk on whitespace
        }

        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small")
        );

        var finalListener = testListener();
        List<EmbeddingRequestChunker.BatchRequestAndListener> batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, 0)
            .batchRequestsWithListeners(finalListener);

        // The very long passage is split into 10000 chunks for inference, so
        // there are 10002 inference requests, resulting in 2001 batches.
        assertThat(batches, hasSize(2001));
        for (int i = 0; i < 2000; i++) {
            assertThat(batches.get(i).batch().inputs().get(), hasSize(5));
        }
        assertThat(batches.get(2000).batch().inputs().get(), hasSize(2));

        // Produce inference results for each request, with increasing weights.
        byte weight = 0;
        for (var batch : batches) {
            var embeddings = new ArrayList<DenseEmbeddingByteResults.Embedding>();
            for (int i = 0; i < batch.batch().requests().size(); i++) {
                weight += 1;
                embeddings.add(new DenseEmbeddingByteResults.Embedding(new byte[] { weight }));
            }
            batch.listener().onResponse(new DenseEmbeddingByteResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(3));

        // The first input has the embedding with weight 1.
        ChunkedInference inference = finalListener.results.getFirst();
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        ChunkedInferenceEmbedding chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.getFirst().inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("1st small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingByteResults.Embedding.class));
        DenseEmbeddingByteResults.Embedding embedding = (DenseEmbeddingByteResults.Embedding) chunkedEmbedding.chunks()
            .getFirst()
            .embedding();
        assertThat(embedding.values(), equalTo(new byte[] { 1 }));

        // The very long passage "word0 word1 ... word199999" is split into 10000 chunks for
        // inference. They get the embeddings with weights 2/1024 ... 10000/16384.
        // Next, they are merged into 512 larger chunks, which consists of 19 or 20 smaller chunks
        // and therefore 380 or 400 words. For each, the average weight is collected.
        inference = finalListener.results.get(1);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(512));

        // The first merged chunk consists of 20 small chunks (so 400 words) and the weight
        // is the average of the weights 2 ... 21, so 11.5, which is rounded to 12.
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), startsWith("word0 word1 "));
        assertThat(getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().getFirst().offset()), endsWith(" word398 word399"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingByteResults.Embedding.class));
        embedding = (DenseEmbeddingByteResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.values(), equalTo(new byte[] { 12 }));

        // The last merged chunk consists of 19 small chunks (so 380 words) and the weight
        // is the average of the weights 9983 ... 10001 modulo 256 (bytes overflowing), so
        // the average of -1, 0, 1, ... , 17, so 8.
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            startsWith(" word199620 word199621 ")
        );
        assertThat(
            getMatchedText(inputs.get(1).inputText(), chunkedEmbedding.chunks().get(511).offset()),
            endsWith(" word199998 word199999")
        );
        assertThat(chunkedEmbedding.chunks().get(511).embedding(), instanceOf(DenseEmbeddingByteResults.Embedding.class));
        embedding = (DenseEmbeddingByteResults.Embedding) chunkedEmbedding.chunks().get(511).embedding();
        assertThat(embedding.values(), equalTo(new byte[] { 8 }));

        // The last input has the token with weight 10002 % 256 = 18
        inference = finalListener.results.get(2);
        assertThat(inference, instanceOf(ChunkedInferenceEmbedding.class));
        chunkedEmbedding = (ChunkedInferenceEmbedding) inference;
        assertThat(chunkedEmbedding.chunks(), hasSize(1));
        assertThat(getMatchedText(inputs.get(2).inputText(), chunkedEmbedding.chunks().getFirst().offset()), equalTo("2nd small"));
        assertThat(chunkedEmbedding.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingByteResults.Embedding.class));
        embedding = (DenseEmbeddingByteResults.Embedding) chunkedEmbedding.chunks().getFirst().embedding();
        assertThat(embedding.values(), equalTo(new byte[] { 18 }));
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
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<DenseEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
            }
            batches.getFirst().listener().onResponse(new DenseEmbeddingFloatResults(embeddings));
        }
        {
            var embeddings = new ArrayList<DenseEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
            }
            batches.get(1).listener().onResponse(new DenseEmbeddingFloatResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.getFirst();
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertThat(
                getMatchedText(inputs.getFirst().inputText(), chunkedFloatResult.chunks().getFirst().offset()),
                equalTo("1st small")
            );
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(6));
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(0).offset()),
                startsWith("passage_input0 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(1).offset()),
                startsWith(" passage_input20 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(2).offset()),
                startsWith(" passage_input40 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(3).offset()),
                startsWith(" passage_input60 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(4).offset()),
                startsWith(" passage_input80 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedFloatResult.chunks().get(5).offset()),
                startsWith(" passage_input100 ")
            );
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(2).inputText(), chunkedFloatResult.chunks().getFirst().offset()), equalTo("2nd small"));
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedFloatResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedFloatResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(3).inputText(), chunkedFloatResult.chunks().getFirst().offset()), equalTo("3rd small"));
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
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<DenseEmbeddingByteResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new DenseEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.getFirst().listener().onResponse(new DenseEmbeddingByteResults(embeddings));
        }
        {
            var embeddings = new ArrayList<DenseEmbeddingByteResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new DenseEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(1).listener().onResponse(new DenseEmbeddingByteResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.getFirst();
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.getFirst().inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("1st small"));
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(6));
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(0).offset()),
                startsWith("passage_input0 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(1).offset()),
                startsWith(" passage_input20 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(2).offset()),
                startsWith(" passage_input40 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(3).offset()),
                startsWith(" passage_input60 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(4).offset()),
                startsWith(" passage_input80 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(5).offset()),
                startsWith(" passage_input100 ")
            );
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(2).inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("2nd small"));
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(3).inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("3rd small"));
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
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput(passageBuilder.toString()),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(2));

        // 4 inputs in 2 batches
        {
            var embeddings = new ArrayList<DenseEmbeddingByteResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new DenseEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.getFirst().listener().onResponse(new DenseEmbeddingBitResults(embeddings));
        }
        {
            var embeddings = new ArrayList<DenseEmbeddingByteResults.Embedding>();
            for (int i = 0; i < 4; i++) { // 4 requests in the 2nd batch
                embeddings.add(new DenseEmbeddingByteResults.Embedding(new byte[] { randomByte() }));
            }
            batches.get(1).listener().onResponse(new DenseEmbeddingBitResults(embeddings));
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(4));
        {
            var chunkedResult = finalListener.results.getFirst();
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.getFirst().inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("1st small"));
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(6));
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(0).offset()),
                startsWith("passage_input0 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(1).offset()),
                startsWith(" passage_input20 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(2).offset()),
                startsWith(" passage_input40 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(3).offset()),
                startsWith(" passage_input60 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(4).offset()),
                startsWith(" passage_input80 ")
            );
            assertThat(
                getMatchedText(inputs.get(1).inputText(), chunkedByteResult.chunks().get(5).offset()),
                startsWith(" passage_input100 ")
            );
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(2).inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("2nd small"));
        }
        {
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedByteResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedByteResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(3).inputText(), chunkedByteResult.chunks().getFirst().offset()), equalTo("3rd small"));
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
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small"),
            new ChunkInferenceInput(passageBuilder.toString())
        );

        var finalListener = testListener();
        var batches = new EmbeddingRequestChunker<>(inputs, batchSize, chunkSize, overlap).batchRequestsWithListeners(finalListener);
        assertThat(batches, hasSize(3));

        // 4 inputs in 3 batches
        {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < batchSize; i++) {
                embeddings.add(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken(randomAlphaOfLength(4), 1.0f)), false));
            }
            batches.getFirst().listener().onResponse(new SparseEmbeddingResults(embeddings));
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
            var chunkedResult = finalListener.results.getFirst();
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertThat(
                getMatchedText(inputs.getFirst().inputText(), chunkedSparseResult.chunks().getFirst().offset()),
                equalTo("1st small")
            );
        }
        {
            var chunkedResult = finalListener.results.get(1);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(1).inputText(), chunkedSparseResult.chunks().getFirst().offset()), equalTo("2nd small"));
        }
        {
            var chunkedResult = finalListener.results.get(2);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(1));
            assertThat(getMatchedText(inputs.get(2).inputText(), chunkedSparseResult.chunks().getFirst().offset()), equalTo("3rd small"));
        }
        {
            // this is the large input split in multiple chunks
            var chunkedResult = finalListener.results.get(3);
            assertThat(chunkedResult, instanceOf(ChunkedInferenceEmbedding.class));
            var chunkedSparseResult = (ChunkedInferenceEmbedding) chunkedResult;
            assertThat(chunkedSparseResult.chunks(), hasSize(9)); // passage is split into 9 chunks, 10 words each
            assertThat(
                getMatchedText(inputs.get(3).inputText(), chunkedSparseResult.chunks().get(0).offset()),
                startsWith("passage_input0 ")
            );
            assertThat(
                getMatchedText(inputs.get(3).inputText(), chunkedSparseResult.chunks().get(1).offset()),
                startsWith(" passage_input10 ")
            );
            assertThat(
                getMatchedText(inputs.get(3).inputText(), chunkedSparseResult.chunks().get(8).offset()),
                startsWith(" passage_input80 ")
            );
        }
    }

    public void testBatchChunksAcrossInputsIsFalse_DoesNotBatchChunksFromSeparateInputs() {
        testBatchChunksAcrossInputs(false, List.of(3, 1, 4));
    }

    public void testBatchChunksAcrossInputsIsTrue_DoesBatchChunksFromSeparateInputs() {
        testBatchChunksAcrossInputs(true, List.of(3, 1, 4));
    }

    public void testBatchChunksAcrossInputsIsTrue_GeneratesMultipleBatches() {
        testBatchChunksAcrossInputs(true, List.of(200, 200, 200));
    }

    public void testBatchChunksAcrossInputsIsFalseAndBatchesLessThanMaxChunkLimit_ThrowsAssertionError() {
        int batchSize = randomIntBetween(1, MAX_BATCH_SIZE - 1);
        List<ChunkInferenceInput> inputs = List.of(new ChunkInferenceInput("This is a test sentence with ten words in total. "));
        var chunkingSettings = new SentenceBoundaryChunkingSettings(10, 0);
        expectThrows(
            AssertionError.class,
            () -> new EmbeddingRequestChunker<>(inputs, batchSize, false, chunkingSettings).batchRequestsWithListeners(testListener())
        );
    }

    private void testBatchChunksAcrossInputs(boolean batchChunksAcrossInputs, List<Integer> batchSizes) {
        int maxChunkSize = 10;
        var testSentence = IntStream.range(0, maxChunkSize).mapToObj(i -> "Word" + i).collect(Collectors.joining(" ")) + ".";
        var chunkingSettings = new SentenceBoundaryChunkingSettings(maxChunkSize, 0);
        var totalBatchSizes = batchSizes.stream().mapToInt(Integer::intValue).sum();
        List<ChunkInferenceInput> inputs = batchSizes.stream()
            .map(i -> new ChunkInferenceInput(String.join(" ", Collections.nCopies(i, testSentence))))
            .toList();

        var finalListener = testListener();
        List<EmbeddingRequestChunker.BatchRequestAndListener> batches = new EmbeddingRequestChunker<>(
            inputs,
            MAX_BATCH_SIZE,
            batchChunksAcrossInputs,
            chunkingSettings
        ).batchRequestsWithListeners(finalListener);

        // If we are batching chunks across inputs, we expect the batches to be filled up to the max batch size.
        // Otherwise, we expect one batch per input.
        int expectedNumberOfBatches = batchChunksAcrossInputs ? (int) Math.ceil((double) totalBatchSizes / MAX_BATCH_SIZE) : inputs.size();
        assertThat(batches, hasSize(expectedNumberOfBatches));
        if (batchChunksAcrossInputs) {
            for (int i = 0; i < batches.size(); i++) {
                var expectedBatchSize = i < batches.size() - 1 ? MAX_BATCH_SIZE : totalBatchSizes - (MAX_BATCH_SIZE * (batches.size() - 1));
                assertThat(batches.get(i).batch().inputs().get(), hasSize(expectedBatchSize));
                batches.get(i)
                    .listener()
                    .onResponse(
                        new DenseEmbeddingFloatResults(
                            List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloatBetween(0, 1, true) }))
                        )
                    );
            }
        } else {
            for (int i = 0; i < batches.size(); i++) {
                assertThat(batches.get(i).batch().inputs().get(), hasSize(batchSizes.get(i)));
                batches.get(i)
                    .listener()
                    .onResponse(
                        new DenseEmbeddingFloatResults(
                            List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloatBetween(0, 1, true) }))
                        )
                    );
            }
        }

        assertNotNull(finalListener.results);
        assertThat(finalListener.results, hasSize(3));
    }

    public void testListenerErrorsWithWrongNumberOfResponses() {
        List<ChunkInferenceInput> inputs = List.of(
            new ChunkInferenceInput("1st small"),
            new ChunkInferenceInput("2nd small"),
            new ChunkInferenceInput("3rd small")
        );

        var failureMessage = new AtomicReference<String>();
        var listener = new ActionListener<List<ChunkedInference>>() {

            @Override
            public void onResponse(List<ChunkedInference> chunkedResults) {
                assertThat(chunkedResults.getFirst(), instanceOf(ChunkedInferenceError.class));
                var error = (ChunkedInferenceError) chunkedResults.getFirst();
                failureMessage.set(error.exception().getMessage());
            }

            @Override
            public void onFailure(Exception e) {
                fail("expected a response with an error");
            }
        };

        var batches = new EmbeddingRequestChunker<>(inputs, 10, 100, 0).batchRequestsWithListeners(listener);
        assertThat(batches, hasSize(1));

        var embeddings = new ArrayList<DenseEmbeddingFloatResults.Embedding>();
        embeddings.add(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
        embeddings.add(new DenseEmbeddingFloatResults.Embedding(new float[] { randomFloat() }));
        batches.getFirst().listener().onResponse(new DenseEmbeddingFloatResults(embeddings));
        assertEquals("Error the number of embedding responses [2] does not equal the number of requests [3]", failureMessage.get());
    }

    public void testDoesNotChunkNonTextInputs_whenChunkingSettingsAreNullInInput() {
        InferenceString imageString = new InferenceString(
            "image chunks",
            randomValueOtherThan(TEXT, () -> randomFrom(InferenceString.DataType.values()))
        );
        ChunkInferenceInput imageInput = new ChunkInferenceInput(imageString, null);
        ChunkInferenceInput textInput = new ChunkInferenceInput(new InferenceString("text chunks", TEXT), null);

        var batches = new EmbeddingRequestChunker<>(List.of(imageInput, textInput), 100, 1, 0).batchRequestsWithListeners(testListener());

        assertThat(batches, hasSize(1));
        var expectedOutput = List.of(imageString, new InferenceString("text", TEXT), new InferenceString(" chunks", TEXT));
        assertThat(batches.getFirst().batch().inputs().get(), is(expectedOutput));
        assertThat(batches.getFirst().batch().inputs().get().getFirst(), is(sameInstance(imageString)));
    }

    public void testDoesNotChunkNonTextInputs_whenChunkingSettingsAreSpecifiedInInput() {
        InferenceString imageString = new InferenceString(
            "image chunks",
            randomValueOtherThan(TEXT, () -> randomFrom(InferenceString.DataType.values()))
        );
        WordBoundaryChunkingSettings chunkingSettings = new WordBoundaryChunkingSettings(1, 0);
        ChunkInferenceInput imageInput = new ChunkInferenceInput(imageString, chunkingSettings);
        ChunkInferenceInput textInput = new ChunkInferenceInput(new InferenceString("text chunks", TEXT), chunkingSettings);

        var batches = new EmbeddingRequestChunker<>(List.of(imageInput, textInput), 100).batchRequestsWithListeners(testListener());

        assertThat(batches, hasSize(1));
        var expectedOutput = List.of(imageString, new InferenceString("text", TEXT), new InferenceString(" chunks", TEXT));
        assertThat(batches.getFirst().batch().inputs().get(), is(expectedOutput));
        assertThat(batches.getFirst().batch().inputs().get().getFirst(), is(sameInstance(imageString)));
    }

    private ChunkedResultsListener testListener() {
        return new ChunkedResultsListener();
    }

    private static String getMatchedText(String text, ChunkedInference.TextOffset offset) {
        return text.substring(offset.start(), offset.end());
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
