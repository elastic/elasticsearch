/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.interceptor;

import co.elastic.elasticsearch.test.CustomRestPlugin;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopyNoNullElements;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomRestPluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopyNoNullElements(super.nodePlugins(), CustomRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testInterceptor() throws Exception {
        var headerValue = randomAlphaOfLengthBetween(4, 12);
        assertThat(doRequest("x-test-interceptor", headerValue), equalTo(headerValue));
        assertThat(doRequest("x-test-interceptor", null), equalTo(null));
    }

    public void testController() throws Exception {
        var headerValue = randomAlphaOfLengthBetween(4, 12);
        assertThat(doRequest("x-test-controller", headerValue), equalTo(headerValue));
        assertThat(doRequest("x-test-controller", null), equalTo(null));
    }

    private String doRequest(String headerName, String headerValue) throws IOException {
        assertThat(headerName, notNullValue());

        var client = getRestClient();
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headerValue != null) {
            options.addHeader(headerName, headerValue);
        }
        var request = new Request("GET", "/_nodes/_local/plugins");
        request.setOptions(options);

        final Response response = client.performRequest(request);
        return response.getHeader(headerName);
    }
}
