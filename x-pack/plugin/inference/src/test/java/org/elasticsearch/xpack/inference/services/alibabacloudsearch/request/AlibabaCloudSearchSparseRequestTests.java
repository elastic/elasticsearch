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
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException, URISyntaxException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            AlibabaCloudSearchSparseModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchSparseServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchSparseTaskSettingsTests.getTaskSettingsMap(null, null),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_WithTaskSettingsInputType() throws IOException, URISyntaxException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            null,
            AlibabaCloudSearchSparseModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchSparseServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchSparseTaskSettingsTests.getTaskSettingsMap(inputType, null),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
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
            AlibabaCloudSearchSparseModelTests.createModel(
                "embedding_test",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchSparseServiceSettingsTests.getServiceSettingsMap("embeddings_test", "host", "default"),
                AlibabaCloudSearchSparseTaskSettingsTests.getTaskSettingsMap(taskSettingInputType, null),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, taskSettingInputType, requestInputType);
    }

    public static AlibabaCloudSearchSparseRequest createRequest(
        List<String> input,
        InputType inputType,
        AlibabaCloudSearchSparseModel model
    ) {
        var account = new AlibabaCloudSearchAccount(model.getSecretSettings().apiKey());
        return new AlibabaCloudSearchSparseRequest(account, input, inputType, model);
    }
}
