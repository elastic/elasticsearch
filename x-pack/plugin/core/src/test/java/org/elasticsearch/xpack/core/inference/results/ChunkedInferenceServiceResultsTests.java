/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class ChunkedInferenceServiceResultsTests extends AbstractWireSerializingTestCase<ChunkedInferenceServiceResults> {
    private enum EmbeddingType {
        SPARSE,
        TEXT_FLOAT,
        TEXT_BYTE
    }

    public void testOf_DifferentTextAndChunkedInferenceListSizesThrowsIllegalArgumentException() {
        var originalText = randomAlphaOfLength(10);
        List<String> originalTextList = List.of(originalText);
        List<ChunkedInference> chunkedInferenceList = List.of(mock(ChunkedInference.class), mock(ChunkedInference.class));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ChunkedInferenceServiceResults.of(originalTextList, chunkedInferenceList)
        );
        assertEquals("Original text and chunked inference list must have the same size.", e.getMessage());
    }

    public void testOf_ChunkedInferenceErrorThrowsElasticsearchStatusException() {
        ChunkedInference chunkedInference = new ChunkedInferenceError(
            new ElasticsearchStatusException("error", RestStatus.INTERNAL_SERVER_ERROR, List.of())
        );
        String originalText = randomAlphaOfLength(10);
        List<String> originalTextList = List.of(originalText);
        List<ChunkedInference> chunkedInferenceList = List.of(chunkedInference);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> ChunkedInferenceServiceResults.of(originalTextList, chunkedInferenceList)
        );
        assertEquals("Received error chunked inference result.", e.getMessage());
        assertEquals(500, e.status().getStatus());
        assertEquals("error", e.getCause().getMessage());
    }

    public void testOf_InvalidChunkedInferenceThrowsIllegalArgumentException() {
        ChunkedInference chunkedInference = mock(ChunkedInference.class);
        String originalText = randomAlphaOfLength(10);
        List<String> originalTextList = List.of(originalText);
        List<ChunkedInference> chunkedInferenceList = List.of(chunkedInference);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ChunkedInferenceServiceResults.of(originalTextList, chunkedInferenceList)
        );
        assertEquals(
            "Received invalid chunked inference result of type "
                + chunkedInference.getClass().getName()
                + " but expected ChunkedInferenceEmbedding",
            e.getMessage()
        );
    }

    public void testOf_SingleValidChunkedInferenceCreatesChunkedInferenceServiceResults() {
        testOf_ValidChunkedInferenceCreatesChunkedInferenceServiceResults(1);
    }

    public void testOf_MultipleValidChunkedInferenceCreatesChunkedInferenceServiceResults() {
        testOf_ValidChunkedInferenceCreatesChunkedInferenceServiceResults(randomIntBetween(2, 10));
    }

    private void testOf_ValidChunkedInferenceCreatesChunkedInferenceServiceResults(int numChunkedInferences) {
        List<String> originalTextList = new ArrayList<>();
        List<ChunkedInference> chunkedInferenceList = new ArrayList<>();
        List<List<EmbeddingResults.Chunk>> chunksPerChunkedInference = new ArrayList<>();
        for (int i = 0; i < numChunkedInferences; i++) {
            String originalText = randomAlphaOfLengthBetween(10, 20);
            var randomNumEmbeddings = randomIntBetween(1, 10);
            var embeddings = new ArrayList<EmbeddingResults.Embedding<?>>();
            for (int j = 0; j < randomNumEmbeddings; j++) {
                embeddings.add(createRandomEmbedding(randomFrom(EmbeddingType.values())));
            }

            var chunks = createChunks(randomNumEmbeddings, originalText.length(), embeddings);
            ChunkedInference chunkedInference = createRandomChunkedInference(chunks);
            originalTextList.add(originalText);
            chunkedInferenceList.add(chunkedInference);
            chunksPerChunkedInference.add(chunks);
        }

        ChunkedInferenceServiceResults chunkedInferenceServiceResults = ChunkedInferenceServiceResults.of(
            originalTextList,
            chunkedInferenceList
        );
        assertEquals(numChunkedInferences, chunkedInferenceServiceResults.results().size());
        for (int i = 0; i < numChunkedInferences; i++) {
            ChunkedInferenceServiceResults.Result result = chunkedInferenceServiceResults.results().get(i);
            var chunksForChunkedInference = chunksPerChunkedInference.get(i);
            assertEquals(chunksForChunkedInference.size(), result.chunks().size());
            for (int j = 0; j < result.chunks().size(); j++) {
                var chunk = result.chunks().get(j);
                assertEquals(
                    originalTextList.get(i)
                        .substring(chunksForChunkedInference.get(j).offset().start(), chunksForChunkedInference.get(j).offset().end() + 1),
                    chunk.chunkText()
                );
                assertEquals(chunksForChunkedInference.get(j).embedding(), chunk.embedding());
            }
        }
    }

    @Override
    protected Writeable.Reader<ChunkedInferenceServiceResults> instanceReader() {
        return ChunkedInferenceServiceResults::new;
    }

    @Override
    protected ChunkedInferenceServiceResults createTestInstance() {
        int numResults = randomIntBetween(1, 10);
        List<ChunkedInferenceServiceResults.Result> results = new ArrayList<>();

        for (int i = 0; i < numResults; i++) {
            var numChunks = randomIntBetween(1, 10);
            var embeddingType = randomFrom(EmbeddingType.values());

            List<ChunkedInferenceServiceResults.ChunkWithChunkText> chunks = new ArrayList<>();
            for (int j = 0; j < numChunks; j++) {
                var embedding = createRandomEmbedding(embeddingType);
                chunks.add(new ChunkedInferenceServiceResults.ChunkWithChunkText(randomAlphaOfLength(10), embedding));
            }

            results.add(new ChunkedInferenceServiceResults.Result(chunks));
        }
        return new ChunkedInferenceServiceResults(results);
    }

    private List<EmbeddingResults.Chunk> createChunks(
        int numChunks,
        int textLength,
        List<? extends EmbeddingResults.Embedding<?>> embeddings
    ) {
        var chunks = new ArrayList<EmbeddingResults.Chunk>();
        var chunkStart = 0;
        for (int i = 0; i < numChunks; i++) {
            var embedding = embeddings.get(i % embeddings.size());
            chunks.add(
                new EmbeddingResults.Chunk(
                    embedding,
                    new ChunkedInference.TextOffset(chunkStart, chunkStart + ((textLength - 1) / numChunks))
                )
            );
            chunkStart += (textLength / numChunks);
        }
        return chunks;
    }

    private ChunkedInference createRandomChunkedInference(List<EmbeddingResults.Chunk> chunks) {
        return new ChunkedInferenceEmbedding(chunks);
    }

    private EmbeddingResults.Embedding<?> createRandomEmbedding(EmbeddingType embeddingType) {
        return switch (embeddingType) {
            case SPARSE -> SparseEmbeddingResultsTests.createRandomEmbedding(randomIntBetween(1, 10));
            case TEXT_FLOAT -> TextEmbeddingFloatResultsTests.createRandomEmbedding();
            case TEXT_BYTE -> TextEmbeddingByteResultsTests.createRandomEmbedding();
        };
    }

    @Override
    protected ChunkedInferenceServiceResults mutateInstance(ChunkedInferenceServiceResults instance) throws IOException {
        List<ChunkedInferenceServiceResults.Result> copy = new ArrayList<>(List.copyOf(instance.results()));
        copy.add(
            new ChunkedInferenceServiceResults.Result(
                List.of(
                    new ChunkedInferenceServiceResults.ChunkWithChunkText(
                        randomAlphaOfLength(10),
                        createRandomEmbedding(randomFrom(EmbeddingType.values()))
                    )
                )
            )
        );
        return new ChunkedInferenceServiceResults(copy);
    }
}
