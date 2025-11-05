/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class LegacyDenseEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<LegacyDenseEmbeddingByteResults> {
    public static LegacyDenseEmbeddingByteResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<LegacyDenseEmbeddingByteResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new LegacyDenseEmbeddingByteResults(embeddingResults);
    }

    private static LegacyDenseEmbeddingByteResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];

        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

        return new LegacyDenseEmbeddingByteResults.Embedding(bytes);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new LegacyDenseEmbeddingByteResults(List.of(new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bytes" : [
                {
                  "embedding" : [
                    23
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new LegacyDenseEmbeddingByteResults(
            List.of(
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 }),
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 24 })
            )
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bytes" : [
                {
                  "embedding" : [
                    23
                  ]
                },
                {
                  "embedding" : [
                    24
                  ]
                }
              ]
            }"""));
    }

    public void testTransformToCoordinationFormat() {
        var results = new LegacyDenseEmbeddingByteResults(
            List.of(
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlDenseEmbeddingResults(LegacyDenseEmbeddingByteResults.TEXT_EMBEDDING_BYTES, new double[] { 23F, 24F }, false),
                    new MlDenseEmbeddingResults(LegacyDenseEmbeddingByteResults.TEXT_EMBEDDING_BYTES, new double[] { 25F, 26F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new LegacyDenseEmbeddingByteResults(
            List.of(
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(2));
    }

    public void testEmbeddingMerge() {
        LegacyDenseEmbeddingByteResults.Embedding embedding1 = new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { 1, 1, -128 });
        LegacyDenseEmbeddingByteResults.Embedding embedding2 = new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { 1, 0, 127 });
        LegacyDenseEmbeddingByteResults.Embedding embedding3 = new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { 0, 0, 100 });
        LegacyDenseEmbeddingByteResults.Embedding mergedEmbedding = embedding1.merge(embedding2);
        assertThat(mergedEmbedding, equalTo(new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { 1, 1, 0 })));
        mergedEmbedding = mergedEmbedding.merge(embedding3);
        assertThat(mergedEmbedding, equalTo(new LegacyDenseEmbeddingByteResults.Embedding(new byte[] { 1, 0, 33 })));
    }

    @Override
    protected Writeable.Reader<LegacyDenseEmbeddingByteResults> instanceReader() {
        return LegacyDenseEmbeddingByteResults::new;
    }

    @Override
    protected LegacyDenseEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected LegacyDenseEmbeddingByteResults mutateInstance(LegacyDenseEmbeddingByteResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new LegacyDenseEmbeddingByteResults(instance.embeddings().subList(0, end));
        } else {
            List<LegacyDenseEmbeddingByteResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new LegacyDenseEmbeddingByteResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationByte(List<byte[]> embeddings) {
        return Map.of(
            LegacyDenseEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(LegacyDenseEmbeddingByteResults.Embedding::new).toList()
        );
    }
}
