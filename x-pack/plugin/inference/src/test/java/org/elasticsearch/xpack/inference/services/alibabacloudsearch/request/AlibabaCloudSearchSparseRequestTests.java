/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException {
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

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        MatcherAssert.assertThat(
            httpPost.getURI().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, null, inputType);
    }

    public void testCreateRequest_WithTaskSettingsInputType() throws IOException {
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

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        MatcherAssert.assertThat(
            httpPost.getURI().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        AlibabaCloudSearchEmbeddingsRequestTests.validateInputType(requestMap, inputType, null);
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
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

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        MatcherAssert.assertThat(
            httpPost.getURI().toString(),
            is("https://host/v3/openapi/workspaces/default/text-sparse-embedding/embeddings_test")
        );
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
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
