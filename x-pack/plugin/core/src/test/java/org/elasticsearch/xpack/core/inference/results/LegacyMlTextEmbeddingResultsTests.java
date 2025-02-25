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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

@SuppressWarnings("deprecation")
public class LegacyMlTextEmbeddingResultsTests extends AbstractWireSerializingTestCase<LegacyTextEmbeddingResults> {
    public static LegacyTextEmbeddingResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<LegacyTextEmbeddingResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new LegacyTextEmbeddingResults(embeddingResults);
    }

    private static LegacyTextEmbeddingResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new LegacyTextEmbeddingResults.Embedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new LegacyTextEmbeddingResults(List.of(new LegacyTextEmbeddingResults.Embedding(new float[] { 0.1F })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding" : [
                {
                  "embedding" : [
                    0.1
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new LegacyTextEmbeddingResults(
            List.of(
                new LegacyTextEmbeddingResults.Embedding(new float[] { 0.1F }),
                new LegacyTextEmbeddingResults.Embedding(new float[] { 0.2F })
            )
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding" : [
                {
                  "embedding" : [
                    0.1
                  ]
                },
                {
                  "embedding" : [
                    0.2
                  ]
                }
              ]
            }"""));
    }

    private static String toJsonString(ToXContentFragment entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject();
        entity.toXContent(builder, null);
        builder.endObject();

        return Strings.toString(builder);
    }

    @Override
    protected Writeable.Reader<LegacyTextEmbeddingResults> instanceReader() {
        return LegacyTextEmbeddingResults::new;
    }

    @Override
    protected LegacyTextEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected LegacyTextEmbeddingResults mutateInstance(LegacyTextEmbeddingResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new LegacyTextEmbeddingResults(instance.embeddings().subList(0, end));
        } else {
            List<LegacyTextEmbeddingResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new LegacyTextEmbeddingResults(embeddings);
        }
    }
}
