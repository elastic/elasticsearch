/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.createModel;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests.getEmbeddingServiceSettings;
import static org.hamcrest.CoreMatchers.is;

public class JinaAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var modelName = "modelName";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var lateChunking = randomBoolean();
        var dimensions = randomNonNegativeInt();
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            createModel(
                null,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(InputType.INGEST, lateChunking),
                "apiKey",
                dimensions
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"input":["abc"],"model":"%s","embedding_type":"%s",\
                        "task":"retrieval.passage","late_chunking":%b,"dimensions":%d}""",
                    modelName,
                    embeddingType.toRequestString(),
                    lateChunking,
                    dimensions
                )
            )
        );
    }

    public void testXContent_WritesOnlyLateChunkingField_WhenItIsTheOnlyOptionalFieldDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            createModel(null, "modelName", null, new JinaAIEmbeddingsTaskSettings(null, false), "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float","task":"retrieval.passage","late_chunking":false}"""));
    }

    public void testXContent_WritesFalseLateChunkingField_WhenLateChunkingSetToTrueButInputExceedsWordCountLimit() throws IOException {
        int wordCount = JinaAIEmbeddingsRequestEntity.MAX_WORD_COUNT_FOR_LATE_CHUNKING + 1;
        var testInput = IntStream.range(0, wordCount / 2).mapToObj(i -> "word" + i).collect(Collectors.joining(" ")) + ".";
        var testInputs = List.of(testInput, testInput, testInput);

        var entity = new JinaAIEmbeddingsRequestEntity(
            testInputs,
            InputType.INTERNAL_INGEST,
            createModel(null, "modelName", null, new JinaAIEmbeddingsTaskSettings(null, true), "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"input":["%s","%s","%s"],"model":"modelName","embedding_type":"float",\
            "task":"retrieval.passage","late_chunking":false}""", testInput, testInput, testInput)));
    }

    public void testXContent_WritesInputTypeField_WhenItIsDefinedOnlyInTaskSettings() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            createModel(null, "modelName", null, new JinaAIEmbeddingsTaskSettings(InputType.SEARCH, null), "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float","task":"retrieval.query"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            createModel(null, "modelName", null, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float"}"""));
    }

    public void testXContent_EmbeddingTypesBit() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.CLUSTERING,
            createModel(null, "model", JinaAIEmbeddingType.BIT, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"separation"}"""));
    }

    public void testXContent_EmbeddingTypesBinary() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.SEARCH,
            createModel(null, "model", JinaAIEmbeddingType.BINARY, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, "apiKey", null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"retrieval.query"}"""));
    }

    public void testXContent_doesNotWriteDimensions_whenDimensionsSetByUserIsFalse() throws IOException {
        var serviceSettings = getEmbeddingServiceSettings("modelName", null, null, 512, null, null, false);
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            createModel(null, serviceSettings, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, null, "apiKey")
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"modelName","embedding_type":"float"}"""));
    }
}
