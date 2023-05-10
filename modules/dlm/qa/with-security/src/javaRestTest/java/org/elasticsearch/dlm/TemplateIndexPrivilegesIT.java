/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.client.Request;
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
    protected Settings restClientSettings() {
        // Note: This user is defined in build.gradle, and assigned the role "manage_dlm". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_dlm", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testAuthorizeIndexTemplateActions() throws IOException {
        makeRequest(client(), requestWithBody("PUT", "/_index_template/test_template", """
            {
                "index_patterns": ["dlm-test*"],
                "data_stream": { }
            }"""), true);
    }

    private Request requestWithBody(String method, String endpoint, String jsonBody) {
        var request = new Request(method, endpoint);
        request.setJsonEntity(jsonBody);
        return request;
    }

    /*
     * This makes the given request with the given client. It asserts a 200 response if expectSuccess is true, and asserts an exception
     * with a 403 response if expectStatus is false.
     */
    private void makeRequest(RestClient client, Request request, boolean expectSuccess) throws IOException {
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }
}
