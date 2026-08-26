/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchEmbeddingsRequestEntity.convertToString;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException, URISyntaxException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_WithTaskSettingInputType() throws IOException, URISyntaxException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            null,
            AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(inputType),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, inputType, null);
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException, URISyntaxException {
        var requestInputType = InputTypeTests.randomSearchAndIngestWithNull();
        var taskSettingInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            requestInputType,
            AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(taskSettingInputType),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, taskSettingInputType, requestInputType);
    }

    public static AlibabaCloudSearchEmbeddingsRequest createRequest(
        List<String> input,
        InputType inputType,
        AlibabaCloudSearchEmbeddingsModel model
    ) {
        var account = new AlibabaCloudSearchAccount(model.getSecretSettings().apiKey());
        return new AlibabaCloudSearchEmbeddingsRequest(account, input, inputType, model);
    }

    public static void validateInputType(Map<String, Object> requestMap, InputType taskSettingsInputType, InputType requestInputType) {
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertToString(requestInputType);
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "input_type", convertedInputType)));
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertToString(taskSettingsInputType);
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "input_type", convertedInputType)));
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"))));
        }
    }
}
