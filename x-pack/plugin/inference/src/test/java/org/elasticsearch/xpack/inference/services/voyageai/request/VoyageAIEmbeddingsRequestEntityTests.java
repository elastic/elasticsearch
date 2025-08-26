/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

public class VoyageAIEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_ServiceSettingsDefined() throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_SEARCH,
            VoyageAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        "https://www.abc.com",
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        2048,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512,
                        VoyageAIServiceSettings.MODEL_ID,
                        "model",
                        VoyageAIEmbeddingsServiceSettings.EMBEDDING_TYPE,
                        "float"
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            ),
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"query","output_dimension":2048,"output_dtype":"float"}"""));
    }

    public void testXContent_WritesAllFields_ServiceSettingsDefined_Int8() throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INGEST,
            VoyageAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        "https://www.abc.com",
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        2048,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512,
                        VoyageAIServiceSettings.MODEL_ID,
                        "model",
                        VoyageAIEmbeddingsServiceSettings.EMBEDDING_TYPE,
                        "int8"
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            ),
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"document","output_dimension":2048,"output_dtype":"int8"}"""));
    }

    public void testXContent_WritesAllFields_ServiceSettingsDefined_Binary() throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_SEARCH,
            VoyageAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        "https://www.abc.com",
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        2048,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512,
                        VoyageAIServiceSettings.MODEL_ID,
                        "model",
                        VoyageAIEmbeddingsServiceSettings.EMBEDDING_TYPE,
                        "binary"
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            ),
            new VoyageAIEmbeddingsTaskSettings(null, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"query","output_dimension":2048,"output_dtype":"binary"}"""));
    }

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            VoyageAIEmbeddingsServiceSettings.EMPTY_SETTINGS,
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","input_type":"document"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new VoyageAIEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            VoyageAIEmbeddingsServiceSettings.EMPTY_SETTINGS,
            VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model"}"""));
    }
}
