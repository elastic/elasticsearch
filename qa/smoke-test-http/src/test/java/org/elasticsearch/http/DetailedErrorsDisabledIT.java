/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import java.io.IOException;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Tests that when disabling detailed errors, a request with the error_trace parameter returns an HTTP 400 response.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class DetailedErrorsDisabledIT extends HttpSmokeTestCase {

    // Build our cluster settings
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED.getKey(), false)
                .build();
    }

    public void testThatErrorTraceParamReturns400() throws IOException {
        Request request = new Request("DELETE", "/");
        request.addParameter("error_trace", "true");
        ResponseException e = expectThrows(ResponseException.class, () ->
            getRestClient().performRequest(request));

        Response response = e.getResponse();
        assertThat(response.getHeader("Content-Type"), is("application/json"));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()),
                   containsString("\"error\":\"error traces in responses are disabled.\""));
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }
}
