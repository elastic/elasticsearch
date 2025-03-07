/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.jinaai;

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

import static org.hamcrest.CoreMatchers.is;

public class JinaAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"float","task":"retrieval.passage"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
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
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model",
            JinaAIEmbeddingType.BIT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary"}"""));
    }

    public void testXContent_EmbeddingTypesBinary() throws IOException {
        var entity = new JinaAIEmbeddingsRequestEntity(
            List.of("abc"),
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model",
            JinaAIEmbeddingType.BINARY
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","embedding_type":"binary"}"""));
    }

    public void testConvertToString_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(
            AssertionError.class,
            () -> JinaAIEmbeddingsRequestEntity.convertToString(InputType.UNSPECIFIED)
        );
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }
}
