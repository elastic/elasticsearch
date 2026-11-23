/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion.AlibabaCloudSearchCompletionRequest;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException, URISyntaxException {
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

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        MatcherAssert.assertThat(
            httpPost.getUri().toString(),
            is("https://host/v3/openapi/workspaces/default/text-generation/completion_test")
        );
        MatcherAssert.assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        MatcherAssert.assertThat(requestMap, is(Map.of("messages", List.of(Map.of("role", "user", "content", "query")))));
    }

    public static AlibabaCloudSearchCompletionRequest createRequest(List<String> input, AlibabaCloudSearchCompletionModel model) {
        var account = new AlibabaCloudSearchAccount(model.getSecretSettings().apiKey());
        return new AlibabaCloudSearchCompletionRequest(account, input, model);
    }
}
