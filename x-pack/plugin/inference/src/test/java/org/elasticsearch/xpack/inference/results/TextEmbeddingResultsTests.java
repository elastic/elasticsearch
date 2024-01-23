/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.inference.results.ByteValue;
import org.elasticsearch.xpack.core.inference.results.FloatValue;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.ML_INFERENCE_REQUEST_INPUT_TYPE_ADDED;
import static org.hamcrest.Matchers.is;

public class TextEmbeddingResultsTests extends AbstractBWCWireSerializationTestCase<TextEmbeddingResults> {

    private enum EmbeddingType {
        FLOAT,
        BYTE
    }

    private static Map<EmbeddingType, Supplier<TextEmbeddingResults.Embedding>> EMBEDDING_TYPE_BUILDERS = Map.of(
        EmbeddingType.FLOAT,
        TextEmbeddingResultsTests::createRandomFloatEmbedding,
        EmbeddingType.BYTE,
        TextEmbeddingResultsTests::createRandomByteEmbedding
    );

    public static TextEmbeddingResults createRandomResults() {
        var embeddingType = randomFrom(EmbeddingType.values());
        var createFunction = EMBEDDING_TYPE_BUILDERS.get(embeddingType);
        assert createFunction != null : "the embeddings type map is missing a value from the EmbeddingType enum";

        return createRandomResults(createFunction);
    }

    public static TextEmbeddingResults createRandomFloatResults() {
        return createRandomResults(TextEmbeddingResultsTests::createRandomFloatEmbedding);
    }

    private static TextEmbeddingResults createRandomResults(Supplier<TextEmbeddingResults.Embedding> creator) {
        int embeddings = randomIntBetween(1, 10);
        List<TextEmbeddingResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(creator.get());
        }

        return new TextEmbeddingResults(embeddingResults);
    }

    private static TextEmbeddingResults.Embedding createRandomByteEmbedding() {
        int columns = randomIntBetween(1, 10);
        List<Byte> bytes = new ArrayList<>(columns);

        for (int i = 0; i < columns; i++) {
            bytes.add(randomByte());
        }

        return TextEmbeddingResults.Embedding.ofBytes(bytes);
    }

    private static TextEmbeddingResults.Embedding createRandomFloatEmbedding() {
        int columns = randomIntBetween(1, 10);
        List<Float> floats = new ArrayList<>(columns);

        for (int i = 0; i < columns; i++) {
            floats.add(randomFloat());
        }

        return TextEmbeddingResults.Embedding.ofFloats(floats);
    }

    public static Map<String, Object> buildExpectationFloats(List<List<Float>> embeddings) {
        return Map.of(
            TextEmbeddingResults.TEXT_EMBEDDING,
            embeddings.stream()
                .map(embedding -> Map.of(TextEmbeddingResults.Embedding.EMBEDDING, embedding.stream().map(FloatValue::new).toList()))
                .toList()
        );
    }

    public static Map<String, Object> buildExpectationBytes(List<List<Byte>> embeddings) {
        return Map.of(
            TextEmbeddingResults.TEXT_EMBEDDING,
            embeddings.stream()
                .map(embedding -> Map.of(TextEmbeddingResults.Embedding.EMBEDDING, embedding.stream().map(ByteValue::new).toList()))
                .toList()
        );
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() {
        var entity = new TextEmbeddingResults(List.of(TextEmbeddingResults.Embedding.ofFloats(List.of(0.1F))));

        MatcherAssert.assertThat(entity.asMap(), is(buildExpectationFloats(List.of(List.of(0.1F)))));

        String xContentResult = Strings.toString(entity, true, true);
        MatcherAssert.assertThat(xContentResult, is("""
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

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding_ForBytes() {
        var entity = new TextEmbeddingResults(List.of(TextEmbeddingResults.Embedding.ofBytes(List.of((byte) 12))));

        MatcherAssert.assertThat(entity.asMap(), is(buildExpectationBytes(List.of(List.of((byte) 12)))));

        String xContentResult = Strings.toString(entity, true, true);
        MatcherAssert.assertThat(xContentResult, is("""
            {
              "text_embedding" : [
                {
                  "embedding" : [
                    12
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() {
        var entity = new TextEmbeddingResults(
            List.of(TextEmbeddingResults.Embedding.ofFloats(List.of(0.1F)), TextEmbeddingResults.Embedding.ofFloats(List.of(0.2F)))
        );

        MatcherAssert.assertThat(entity.asMap(), is(buildExpectationFloats(List.of(List.of(0.1F), List.of(0.2F)))));

        String xContentResult = Strings.toString(entity, true, true);
        MatcherAssert.assertThat(xContentResult, is("""
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

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings_ForBytes() {
        var entity = new TextEmbeddingResults(
            List.of(TextEmbeddingResults.Embedding.ofBytes(List.of((byte) 12)), TextEmbeddingResults.Embedding.ofBytes(List.of((byte) 34)))
        );

        MatcherAssert.assertThat(entity.asMap(), is(buildExpectationBytes(List.of(List.of((byte) 12), List.of((byte) 34)))));

        String xContentResult = Strings.toString(entity, true, true);
        MatcherAssert.assertThat(xContentResult, is("""
            {
              "text_embedding" : [
                {
                  "embedding" : [
                    12
                  ]
                },
                {
                  "embedding" : [
                    34
                  ]
                }
              ]
            }"""));
    }

    public void testTransformToCoordinationFormat() {
        var results = new TextEmbeddingResults(
            List.of(
                TextEmbeddingResults.Embedding.ofFloats(List.of(0.1F, 0.2F)),
                TextEmbeddingResults.Embedding.ofFloats(List.of(0.3F, 0.4F))
            )
        ).transformToCoordinationFormat();

        MatcherAssert.assertThat(
            results,
            is(
                List.of(
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 0.1F, 0.2F },
                        false
                    ),
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 0.3F, 0.4F },
                        false
                    )
                )
            )
        );
    }

    public void testTransformToCoordinationFormat_FromBytes() {
        var results = new TextEmbeddingResults(
            List.of(
                TextEmbeddingResults.Embedding.ofBytes(List.of((byte) 12, (byte) 34)),
                TextEmbeddingResults.Embedding.ofBytes(List.of((byte) 56, (byte) -78))
            )
        ).transformToCoordinationFormat();

        MatcherAssert.assertThat(
            results,
            is(
                List.of(
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 12F, 34F },
                        false
                    ),
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 56F, -78F },
                        false
                    )
                )
            )
        );
    }

    public void testSerializesToFloats_WhenVersionIsPriorToByteSupport() throws IOException {
        var instance = createRandomResults(TextEmbeddingResultsTests::createRandomByteEmbedding);
        var modifiedForOlderVersion = mutateInstanceForVersion(instance, ML_INFERENCE_REQUEST_INPUT_TYPE_ADDED);

        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), ML_INFERENCE_REQUEST_INPUT_TYPE_ADDED);
        assertOnBWCObject(copy, modifiedForOlderVersion, ML_INFERENCE_REQUEST_INPUT_TYPE_ADDED);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<TextEmbeddingResults> instanceReader() {
        return TextEmbeddingResults::new;
    }

    @Override
    protected TextEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextEmbeddingResults mutateInstance(TextEmbeddingResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new TextEmbeddingResults(instance.embeddings().subList(0, end));
        } else {
            List<TextEmbeddingResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomFloatEmbedding());
            return new TextEmbeddingResults(embeddings);
        }
    }

    @Override
    protected TextEmbeddingResults mutateInstanceForVersion(TextEmbeddingResults instance, TransportVersion version) {
        if (version.before(TransportVersions.ML_INFERENCE_COHERE_EMBEDDINGS_ADDED)) {
            return convertToFloatEmbeddings(instance);
        }

        return instance;
    }

    public TextEmbeddingResults convertToFloatEmbeddings(TextEmbeddingResults results) {
        var floatEmbeddings = results.embeddings()
            .stream()
            .map(embedding -> TextEmbeddingResults.Embedding.ofFloats(embedding.toFloats()))
            .toList();

        return new TextEmbeddingResults(floatEmbeddings);
    }
}
