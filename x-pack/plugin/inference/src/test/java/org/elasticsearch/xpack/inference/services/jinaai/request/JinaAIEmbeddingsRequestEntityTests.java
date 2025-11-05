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
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;

public class JinaAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"float","task":"retrieval.passage","late_chunking":true}"""));
    }

    public void testXContent_WritesOnlyLateChunkingField_WhenItIsTheOnlyOptionalFieldDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new JinaAIEmbeddingsTaskSettings(null, false),
            "model",
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","task":"retrieval.passage","late_chunking":false}"""));
    }

    public void testXContent_WritesFalseLateChunkingField_WhenLateChunkingSetToTrueButInputExceedsWordCountLimit() throws IOException {
        int wordCount = JinaAIEmbeddingsRequestEntity.MAX_WORD_COUNT_FOR_LATE_CHUNKING + 1;
        var testInput = IntStream.range(0, wordCount / 2).mapToObj(i -> "word" + i).collect(Collectors.joining(" ")) + ".";
        var testInputs = List.of(testInput, testInput, testInput);

        var entity = new JinaAIEmbeddingsRequestEntity(
            testInputs,
            InputType.INTERNAL_INGEST,
            new JinaAIEmbeddingsTaskSettings(null, true),
            "model",
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"input":["%s","%s","%s"],"model":"model","task":"retrieval.passage","late_chunking":false}""",
                    testInput,
                    testInput,
                    testInput
                )
            )
        );
    }

    public void testXContent_WritesInputTypeField_WhenItIsDefinedOnlyInTaskSettings() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            new JinaAIEmbeddingsTaskSettings(InputType.SEARCH, null),
            "model",
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","task":"retrieval.query"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"float"}"""));
    }

    public void testXContent_EmbeddingTypesBit() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.CLUSTERING,
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model",
            JinaAIEmbeddingType.BIT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"separation"}"""));
    }

    public void testXContent_EmbeddingTypesBinary() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.SEARCH,
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model",
            JinaAIEmbeddingType.BINARY
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary","task":"retrieval.query"}"""));
    }
}
