/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test a rest action that sets special response headers
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ResponseHeaderPluginIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestResponseHeaderPlugin.class);
    }

    public void testThatSettingHeadersWorks() throws IOException {
        ensureGreen();
        try {
            getRestClient().performRequest(new Request("GET", "/_protected"));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), equalTo(401));
            assertThat(response.getHeader("Secret"), equalTo("required"));
        }

        Request request = new Request("GET", "/_protected");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Secret", "password");
        request.setOptions(options);
        Response authResponse = getRestClient().performRequest(request);
        assertThat(authResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(authResponse.getHeader("Secret"), equalTo("granted"));
    }
}
