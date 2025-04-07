/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion.AlibabaCloudSearchCompletionRequest;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException {
        var request = createRequest(
            List.of("query"),
            AlibabaCloudSearchCompletionModelTests.createModel(
                "completion_test",
                TaskType.COMPLETION,
                AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("completion_test", "host", "default"),
                AlibabaCloudSearchCompletionTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        MatcherAssert.assertThat(
            httpPost.getURI().toString(),
            is("https://host/v3/openapi/workspaces/default/text-generation/completion_test")
        );
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap, is(Map.of("messages", List.of(Map.of("role", "user", "content", "query")))));
    }

    public static AlibabaCloudSearchCompletionRequest createRequest(List<String> input, AlibabaCloudSearchCompletionModel model) {
        var account = new AlibabaCloudSearchAccount(model.getSecretSettings().apiKey());
        return new AlibabaCloudSearchCompletionRequest(account, input, model);
    }
}
