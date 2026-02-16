/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class VoyageAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_ServiceSettingsDefined() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            "https://www.abc.com",
            "api_key",
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            512,
            2048,
            "model"
        );

        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_SEARCH,
            model
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"query","output_dimension":2048,"output_dtype":"float"}"""));
    }

    public void testXContent_WritesAllFields_ServiceSettingsDefined_Int8() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            "https://www.abc.com",
            "api_key",
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            512,
            2048,
            "model",
            org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType.INT8
        );

        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INGEST,
            model
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"document","output_dimension":2048,"output_dtype":"int8"}"""));
    }

    public void testXContent_WritesAllFields_ServiceSettingsDefined_Binary() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            "https://www.abc.com",
            "api_key",
            new VoyageAIEmbeddingsTaskSettings(null, null),
            512,
            2048,
            "model",
            org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType.BINARY
        );

        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            InputType.INTERNAL_SEARCH,
            model
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"query","output_dimension":2048,"output_dtype":"binary"}"""));
    }

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            "https://www.abc.com",
            "api_key",
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model"
        );

        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            model
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"document","output_dtype":"float"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            "https://www.abc.com",
            "api_key",
            VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            null,
            "model"
        );

        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            null,
            model
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","output_dtype":"float"}"""));
    }
}
