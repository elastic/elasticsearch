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

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CorsNotSetIT extends HttpSmokeTestCase {

    public void testCorsSettingDefaultBehaviourDoesNotReturnAnything() throws IOException {
        String corsValue = "http://localhost:9200";
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        options.addHeader("Origin", corsValue);
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
        assertThat(response.getHeader("Access-Control-Allow-Credentials"), nullValue());
    }

    public void testThatOmittingCorsHeaderDoesNotReturnAnything() throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
        assertThat(response.getHeader("Access-Control-Allow-Credentials"), nullValue());
    }
}
