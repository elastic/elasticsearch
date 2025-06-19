/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v1;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CohereV1EmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            CohereEmbeddingsModelTests.createModel(null, "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, null, null)
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("https://api.cohere.ai/v1/embed"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("float")));
        validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            CohereEmbeddingsModelTests.createModel(
                "http://localhost:8080",
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START),
                null,
                null,
                "model",
                CohereEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("http://localhost:8080/v1/embed"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("model"), is("model"));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("float")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("start"));
        validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_WithTaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            null,
            CohereEmbeddingsModelTests.createModel(
                null,
                "secret",
                new CohereEmbeddingsTaskSettings(inputType, CohereTruncation.END),
                null,
                null,
                null,
                null
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("float")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("end"));
        validateInputType(requestMap, inputType, null);
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomSearchAndIngestWithNull();
        var taskSettingInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            requestInputType,
            CohereEmbeddingsModelTests.createModel(
                null,
                "secret",
                new CohereEmbeddingsTaskSettings(taskSettingInputType, CohereTruncation.END),
                null,
                null,
                null,
                null
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("https://api.cohere.ai/v1/embed"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("float")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("end"));
        validateInputType(requestMap, taskSettingInputType, requestInputType);
    }

    public void testCreateRequest_InputTypeSearch_EmbeddingTypeInt8_TruncateEnd() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            CohereEmbeddingsModelTests.createModel(
                null,
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END),
                null,
                null,
                "model",
                CohereEmbeddingType.INT8
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("https://api.cohere.ai/v1/embed"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("model"), is("model"));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("int8")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("end"));
        validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_InputTypeSearch_EmbeddingTypeBit_TruncateEnd() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            CohereEmbeddingsModelTests.createModel(
                null,
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END),
                null,
                null,
                "model",
                CohereEmbeddingType.BIT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("model"), is("model"));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("binary")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("end"));
        validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_TruncateNone() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            CohereEmbeddingsModelTests.createModel(
                null,
                "secret",
                new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE),
                null,
                null,
                null,
                null
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap.get("texts"), is(List.of("abc")));
        MatcherAssert.assertThat(requestMap.get("embedding_types"), is(List.of("float")));
        MatcherAssert.assertThat(requestMap.get("truncate"), is("none"));
        validateInputType(requestMap, null, inputType);
    }

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = createRequest(
            "model",
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.START),
            CohereEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"model":"model","input_type":"search_document","embedding_types":["float"],"truncate":"start"}"""));
    }

    public void testXContent_TaskSettingsInputType_EmbeddingTypesInt8_TruncateNone() throws IOException {
        var entity = createRequest(
            "model",
            List.of("abc"),
            null,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            CohereEmbeddingType.INT8
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["int8"],"truncate":"none"}"""));
    }

    public void testXContent_InternalInputType_EmbeddingTypesByte_TruncateNone() throws IOException {
        var entity = createRequest(
            "model",
            List.of("abc"),
            InputType.INTERNAL_SEARCH,
            new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE),
            CohereEmbeddingType.BYTE
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["int8"],"truncate":"none"}"""));
    }

    public void testXContent_InputTypeSearch_EmbeddingTypesBinary_TruncateNone() throws IOException {
        var entity = createRequest(
            "model",
            List.of("abc"),
            InputType.SEARCH,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            CohereEmbeddingType.BINARY
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["binary"],"truncate":"none"}"""));
    }

    public void testXContent_InputTypeSearch_EmbeddingTypesBit_TruncateNone() throws IOException {
        var entity = createRequest(
            "model",
            List.of("abc"),
            null,
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE),
            CohereEmbeddingType.BIT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"model":"model","input_type":"search_query","embedding_types":["binary"],"truncate":"none"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = createRequest(null, List.of("abc"), null, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"texts":["abc"],"embedding_types":["float"]}"""));
    }

    public static CohereV1EmbeddingsRequest createRequest(List<String> input, InputType inputType, CohereEmbeddingsModel model) {
        return new CohereV1EmbeddingsRequest(input, inputType, model);
    }

    public static CohereV1EmbeddingsRequest createRequest(
        String modelId,
        List<String> input,
        InputType inputType,
        CohereEmbeddingsTaskSettings taskSettings,
        CohereEmbeddingType embeddingType
    ) {
        var model = CohereEmbeddingsModelTests.createModel(null, "secret", taskSettings, null, null, modelId, embeddingType);
        return new CohereV1EmbeddingsRequest(input, inputType, model);
    }

    private void validateInputType(Map<String, Object> requestMap, InputType taskSettingsInputType, InputType requestInputType) {
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = CohereUtils.inputTypeToString(requestInputType);
            assertThat(requestMap.get("input_type"), is(convertedInputType));
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = CohereUtils.inputTypeToString(taskSettingsInputType);
            assertThat(requestMap.get("input_type"), is(convertedInputType));
        }
    }
}
