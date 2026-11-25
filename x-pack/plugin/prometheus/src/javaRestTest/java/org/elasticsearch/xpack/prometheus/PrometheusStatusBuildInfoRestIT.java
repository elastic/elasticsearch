/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the {@code GET /_prometheus/api/v1/status/buildinfo} endpoint.
 */
public class PrometheusStatusBuildInfoRestIT extends AbstractPrometheusRestIT {

    public void testBuildInfoResponse() throws Exception {
        assertBuildInfo("/_prometheus/api/v1/status/buildinfo");
    }

    public void testBuildInfoResponseScopedIndex() throws Exception {
        assertBuildInfo("/_prometheus/metrics-generic.prometheus-default/api/v1/status/buildinfo");
    }

    private void assertBuildInfo(String path) throws Exception {
        Request request = new Request("GET", path);
        addReadAuth(request);

        Response response = client().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        ObjectPath op = ObjectPath.createFromResponse(response);
        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("data.application"), equalTo("Elasticsearch"));
        assertThat(op.evaluate("data.version"), equalTo(Build.current().version()));
        assertThat(op.evaluate("data.revision"), equalTo(Build.current().hash()));
    }
}
