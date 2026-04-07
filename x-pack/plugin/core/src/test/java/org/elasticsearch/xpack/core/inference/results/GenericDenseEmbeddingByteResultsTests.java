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

public class GenericDenseEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<GenericDenseEmbeddingByteResults> {
    public static GenericDenseEmbeddingByteResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<GenericDenseEmbeddingByteResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new GenericDenseEmbeddingByteResults(embeddingResults);
    }

    private static GenericDenseEmbeddingByteResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];

        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

        return new GenericDenseEmbeddingByteResults.Embedding(bytes);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new GenericDenseEmbeddingByteResults(
            List.of(new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 }))
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "embeddings_bytes" : [
                {
                  "embedding" : [
                    23
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new GenericDenseEmbeddingByteResults(
            List.of(
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23 }),
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 24 })
            )
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "embeddings_bytes" : [
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
        var results = new GenericDenseEmbeddingByteResults(
            List.of(
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlDenseEmbeddingResults(GenericDenseEmbeddingByteResults.EMBEDDINGS_BYTES, new double[] { 23F, 24F }, false),
                    new MlDenseEmbeddingResults(GenericDenseEmbeddingByteResults.EMBEDDINGS_BYTES, new double[] { 25F, 26F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new GenericDenseEmbeddingByteResults(
            List.of(
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new GenericDenseEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(2));
    }

    public void testEmbeddingMerge() {
        GenericDenseEmbeddingByteResults.Embedding embedding1 = new GenericDenseEmbeddingByteResults.Embedding(new byte[] { 1, 1, -128 });
        GenericDenseEmbeddingByteResults.Embedding embedding2 = new GenericDenseEmbeddingByteResults.Embedding(new byte[] { 1, 0, 127 });
        GenericDenseEmbeddingByteResults.Embedding embedding3 = new GenericDenseEmbeddingByteResults.Embedding(new byte[] { 0, 0, 100 });
        GenericDenseEmbeddingByteResults.Embedding mergedEmbedding = embedding1.merge(embedding2);
        assertThat(mergedEmbedding, equalTo(new GenericDenseEmbeddingByteResults.Embedding(new byte[] { 1, 1, 0 })));
        mergedEmbedding = mergedEmbedding.merge(embedding3);
        assertThat(mergedEmbedding, equalTo(new GenericDenseEmbeddingByteResults.Embedding(new byte[] { 1, 0, 33 })));
    }

    @Override
    protected Writeable.Reader<GenericDenseEmbeddingByteResults> instanceReader() {
        return GenericDenseEmbeddingByteResults::new;
    }

    @Override
    protected GenericDenseEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected GenericDenseEmbeddingByteResults mutateInstance(GenericDenseEmbeddingByteResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new GenericDenseEmbeddingByteResults(instance.embeddings().subList(0, end));
        } else {
            List<GenericDenseEmbeddingByteResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new GenericDenseEmbeddingByteResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationByte(List<List<Byte>> embeddings) {
        return Map.of(
            GenericDenseEmbeddingByteResults.EMBEDDINGS_BYTES,
            embeddings.stream().map(embedding -> Map.of(EmbeddingResults.EMBEDDING, embedding)).toList()
        );
    }
}
