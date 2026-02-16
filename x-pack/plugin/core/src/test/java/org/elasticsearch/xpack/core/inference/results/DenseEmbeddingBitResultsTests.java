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

import static org.hamcrest.Matchers.is;

public class DenseEmbeddingBitResultsTests extends AbstractWireSerializingTestCase<DenseEmbeddingBitResults> {
    public static DenseEmbeddingBitResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<DenseEmbeddingByteResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new DenseEmbeddingBitResults(embeddingResults);
    }

    private static DenseEmbeddingByteResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];

        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

        return new DenseEmbeddingByteResults.Embedding(bytes);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new DenseEmbeddingBitResults(List.of(new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bits" : [
                {
                  "embedding" : [
                    23
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new DenseEmbeddingBitResults(
            List.of(
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 }),
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 24 })
            )
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bits" : [
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
        var results = new DenseEmbeddingBitResults(
            List.of(
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlDenseEmbeddingResults(DenseEmbeddingBitResults.TEXT_EMBEDDING_BITS, new double[] { 23F, 24F }, false),
                    new MlDenseEmbeddingResults(DenseEmbeddingBitResults.TEXT_EMBEDDING_BITS, new double[] { 25F, 26F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new DenseEmbeddingBitResults(
            List.of(
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new DenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(16));
    }

    @Override
    protected Writeable.Reader<DenseEmbeddingBitResults> instanceReader() {
        return DenseEmbeddingBitResults::new;
    }

    @Override
    protected DenseEmbeddingBitResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected DenseEmbeddingBitResults mutateInstance(DenseEmbeddingBitResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new DenseEmbeddingBitResults(instance.embeddings().subList(0, end));
        } else {
            List<DenseEmbeddingByteResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new DenseEmbeddingBitResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationByte(List<List<Byte>> embeddings) {
        return Map.of(
            DenseEmbeddingBitResults.TEXT_EMBEDDING_BITS,
            embeddings.stream().map(embedding -> Map.of(EmbeddingResults.EMBEDDING, embedding)).toList()
        );
    }
}
