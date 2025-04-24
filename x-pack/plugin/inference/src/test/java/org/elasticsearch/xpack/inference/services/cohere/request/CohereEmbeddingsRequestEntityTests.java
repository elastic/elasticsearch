/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class CohereEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.START),
            "model",
            CohereEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"],"model":"model","input_type":"search_document","embedding_types":["float"],"truncate":"start"}"""));
    }

    public void testXContent_TaskSettingsInputType_EmbeddingTypesInt8_TruncateNone() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            "model",
            CohereEmbeddingType.INT8
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["int8"],"truncate":"none"}"""));
    }

    public void testXContent_InternalInputType_EmbeddingTypesByte_TruncateNone() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_SEARCH,
            new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE),
            "model",
            CohereEmbeddingType.BYTE
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["int8"],"truncate":"none"}"""));
    }

    public void testXContent_InputTypeSearch_EmbeddingTypesBinary_TruncateNone() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.SEARCH,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            "model",
            CohereEmbeddingType.BINARY
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["binary"],"truncate":"none"}"""));
    }

    public void testXContent_InputTypeSearch_EmbeddingTypesBit_TruncateNone() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            "model",
            CohereEmbeddingType.BIT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["binary"],"truncate":"none"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new CohereEmbeddingsRequestEntity(List.of("abc"), null, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"texts":["abc"]}"""));
    }

    public void testConvertToString_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(
            AssertionError.class,
            () -> CohereEmbeddingsRequestEntity.convertToString(InputType.UNSPECIFIED)
        );
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }
}
