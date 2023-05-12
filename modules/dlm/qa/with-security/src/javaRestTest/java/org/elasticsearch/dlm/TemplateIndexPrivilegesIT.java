/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TemplateIndexPrivilegesIT extends ESRestTestCase {

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testAuthorizeIndexTemplateActions() throws IOException {
        // User with cluster privilege has access to everything
        makeRequest("user_with_cluster", client(), requestWithBody("PUT", "/_index_template/test_template_1", """
            {
                "index_patterns": ["test-*"],
                "priority": 1,
                "data_stream": { }
            }"""), true);
        makeRequest("user_with_cluster", client(), requestWithBody("PUT", "/_index_template/test_template_2", """
            {
                "index_patterns": ["other-*"],
                "priority": 2,
                "data_stream": { }
            }"""), true);

        // User with index privilege only has access to specific index patterns
        makeRequest("user_with_index", client(), requestWithBody("PUT", "/_index_template/test_template_3", """
            {
                "index_patterns": ["test-*"],
                "priority": 3,
                "data_stream": { }
            }"""), true);
        makeRequest("user_with_index", client(), requestWithBody("PUT", "/_index_template/test_template_4", """
            {
                "index_patterns": ["other-*"],
                "priority": 4,
                "data_stream": { }
            }"""), false);
        // Denied because test_template_2 has index patterns `other-*`
        makeRequest("user_with_index", client(), requestWithBody("PUT", "/_index_template/test_template_2", """
            {
                "index_patterns": ["test-*"],
                "priority": 2,
                "data_stream": { }
            }"""), false);
    }

    private Request requestWithBody(String method, String endpoint, String jsonBody) {
        var request = new Request(method, endpoint);
        request.setJsonEntity(jsonBody);
        return request;
    }

    private void makeRequest(String user, RestClient client, Request request, boolean expectSuccess) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(user, new SecureString("x-pack-test-password".toCharArray())))
        );
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }
}
