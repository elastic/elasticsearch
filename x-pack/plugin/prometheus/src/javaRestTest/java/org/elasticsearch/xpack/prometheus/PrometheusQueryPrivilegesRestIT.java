/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * The endpoints that take PromQL parse it where ES|QL parses a query string, once the caller is authorized to run ES|QL.
 * Parsing a query can be costly, so a caller that may only write, such as a Prometheus remote write client, is refused
 * before its query is parsed: it gets a {@code 403} for an invalid query, where one that may read gets a {@code 400}.
 */
public class PrometheusQueryPrivilegesRestIT extends AbstractPrometheusRestIT {

    private static final String START = "2026-01-01T00:00:00Z";
    private static final String END = "2026-01-01T01:00:00Z";
    private static final String INVALID_QUERY = "up{";
    private static final String INVALID_SELECTOR = "{";

    public void testInstantQuery() throws IOException {
        assertParsedOnlyOnceAuthorized(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", INVALID_QUERY),
            new BasicNameValuePair("time", START)
        );
    }

    /** A string literal reads no index, but is still only answered to a caller that may run ES|QL. */
    public void testInstantQueryStringLiteral() throws IOException {
        var params = new NameValuePair[] { new BasicNameValuePair("query", "\"a string\""), new BasicNameValuePair("time", START) };
        assertThat(status(prometheusGetRequest("/_prometheus/api/v1/query", writeApiKey, params)), equalTo(403));
        var response = ObjectPath.createFromResponse(
            client().performRequest(prometheusGetRequest("/_prometheus/api/v1/query", readApiKey, params))
        );
        assertThat(response.evaluate("data.resultType"), equalTo("string"));
        assertThat(response.evaluate("data.result.1"), equalTo("a string"));
    }

    public void testRangeQuery() throws IOException {
        assertParsedOnlyOnceAuthorized(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", INVALID_QUERY),
            new BasicNameValuePair("start", START),
            new BasicNameValuePair("end", END),
            new BasicNameValuePair("step", "1m")
        );
    }

    public void testQueryExemplars() throws IOException {
        assertParsedOnlyOnceAuthorized(
            "/_prometheus/api/v1/query_exemplars",
            new BasicNameValuePair("query", INVALID_QUERY),
            new BasicNameValuePair("start", START),
            new BasicNameValuePair("end", END)
        );
    }

    public void testSeries() throws IOException {
        assertParsedOnlyOnceAuthorized("/_prometheus/api/v1/series", new BasicNameValuePair("match[]", INVALID_SELECTOR));
    }

    public void testLabels() throws IOException {
        assertParsedOnlyOnceAuthorized("/_prometheus/api/v1/labels", new BasicNameValuePair("match[]", INVALID_SELECTOR));
    }

    public void testLabelValues() throws IOException {
        assertParsedOnlyOnceAuthorized("/_prometheus/api/v1/label/job/values", new BasicNameValuePair("match[]", INVALID_SELECTOR));
    }

    private void assertParsedOnlyOnceAuthorized(String path, NameValuePair... params) throws IOException {
        assertThat("refused before the query is parsed", status(prometheusGetRequest(path, writeApiKey, params)), equalTo(403));
        assertThat("the query is parsed once authorized", status(prometheusGetRequest(path, readApiKey, params)), equalTo(400));
    }

    private static int status(Request request) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        return e.getResponse().getStatusLine().getStatusCode();
    }
}
