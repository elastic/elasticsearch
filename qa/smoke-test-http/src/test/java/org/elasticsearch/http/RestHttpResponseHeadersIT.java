/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Refer to
 * <a href="https://github.com/elastic/elasticsearch/issues/15335">Unsupported
 * methods on REST endpoints should respond with status code 405</a> for more
 * information.
 */
public class RestHttpResponseHeadersIT extends ESRestTestCase {

    /**
     * For an OPTIONS request to a valid REST endpoint, verify that a 200 HTTP
     * response code is returned, and that the response 'Allow' header includes
     * a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-9.2">HTTP/1.1 - 9.2
     * - Options</a>).
     */
    public void testValidEndpointOptionsResponseHttpHeader() throws Exception {
        Response response = client().performRequest(new Request("OPTIONS", "/_tasks"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Allow"), notNullValue());
        List<String> responseAllowHeaderStringArray =
                Arrays.asList(response.getHeader("Allow").split(","));
        assertThat(responseAllowHeaderStringArray, containsInAnyOrder("GET"));
    }

    /**
     * For requests to a valid REST endpoint using an unsupported HTTP method,
     * verify that a 405 HTTP response code is returned, and that the response
     * 'Allow' header includes a list of valid HTTP methods for the endpoint
     * (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    public void testUnsupportedMethodResponseHttpHeader() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/_tasks"));
            fail("Request should have failed with 405 error");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), is(405));
            assertThat(response.getHeader("Allow"), notNullValue());
            List<String> responseAllowHeaderStringArray =
                    Arrays.asList(response.getHeader("Allow").split(","));
            assertThat(responseAllowHeaderStringArray, containsInAnyOrder("GET"));
            assertThat(EntityUtils.toString(response.getEntity()),
                containsString("Incorrect HTTP method for uri [/_tasks] and method [DELETE], allowed: [GET]"));
        }
    }

    /**
     * Test if a POST request to /{index}/_settings matches the update settings
     * handler for /{index}/_settings, and returns a 405 error (see
     * <a href="https://github.com/elastic/elasticsearch/issues/17853">Issue
     * 17853</a> for more information).
     */
    public void testIndexSettingsPostRequest() throws Exception {
        client().performRequest(new Request("PUT", "/testindex"));
        try {
            client().performRequest(new Request("POST", "/testindex/_settings"));
            fail("Request should have failed with 405 error");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), is(405));
            assertThat(response.getHeader("Allow"), notNullValue());
            List<String> responseAllowHeaderStringArray =
                    Arrays.asList(response.getHeader("Allow").split(","));
            assertThat(responseAllowHeaderStringArray, containsInAnyOrder("PUT", "GET"));
            assertThat(EntityUtils.toString(response.getEntity()),
                containsString("Incorrect HTTP method for uri [/testindex/_settings] and method [POST], allowed:"));
            assertThat(EntityUtils.toString(response.getEntity()), containsString("GET"));
            assertThat(EntityUtils.toString(response.getEntity()), containsString("PUT"));
        }
    }

}
