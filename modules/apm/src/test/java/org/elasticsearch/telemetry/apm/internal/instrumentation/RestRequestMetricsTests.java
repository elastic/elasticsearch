/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RestRequestMetricsTests extends ESTestCase {

    private static final String SEARCH_DURATION_METRIC = "es.rest.search.duration";

    private final RecordingMeterRegistry registry = new RecordingMeterRegistry();
    private final RestRequestMetrics metrics = new RestRequestMetrics(registry);

    public void test_measuredRoutes_recordDuration() {
        record Case(RestRequest.Method method, String path, String route) {}

        var cases = List.of(
            new Case(RestRequest.Method.GET, "/_search", "_search"),
            new Case(RestRequest.Method.POST, "/_search", "_search"),
            new Case(RestRequest.Method.GET, "/my-index/_search", "{index}/_search"),
            new Case(RestRequest.Method.POST, "/my-index/_search", "{index}/_search"),
            new Case(RestRequest.Method.GET, "/my-index/_knn_search", "{index}/_knn_search"),
            new Case(RestRequest.Method.POST, "/my-index/_knn_search", "{index}/_knn_search")
        );

        for (var c : cases) {
            var ctx = threadContext();
            metrics.start(ctx, request(c.method, c.path), c.route);
            metrics.prepareEnd(ctx, response(RestStatus.OK)).close();
        }

        var recorded = recordings();
        assertEquals(6, recorded.size());
        for (var m : recorded) {
            assertThat(m.getDouble(), greaterThanOrEqualTo(0.0));
        }
    }

    public void test_leadingSlashStripped() {
        var ctx = threadContext();
        metrics.start(ctx, request(RestRequest.Method.GET, "/_search"), "/_search");
        metrics.prepareEnd(ctx, response(RestStatus.OK)).close();

        assertEquals(1, recordings().size());
    }

    public void test_nullRoute_noRecording() {
        var ctx = threadContext();
        metrics.start(ctx, request(RestRequest.Method.GET, "/_search"), null);
        metrics.prepareEnd(ctx, response(RestStatus.OK)).close();

        assertEquals(0, recordings().size());
    }

    public void test_unmeasuredRoute_noRecording() {
        var ctx = threadContext();
        metrics.start(ctx, request(RestRequest.Method.GET, "/_cluster/health"), "_cluster/health");
        metrics.prepareEnd(ctx, response(RestStatus.OK)).close();

        assertEquals(0, recordings().size());
    }

    public void test_prepareEndWithoutStart_noRecording() {
        var ctx = threadContext();
        metrics.prepareEnd(ctx, response(RestStatus.OK)).close();

        assertEquals(0, recordings().size());
    }

    public void test_statusCodeAttribute() {
        var ctx = threadContext();
        metrics.start(ctx, request(RestRequest.Method.GET, "/_search"), "_search");
        metrics.prepareEnd(ctx, response(RestStatus.BAD_REQUEST)).close();

        var recorded = recordings();
        assertEquals(1, recorded.size());
        assertEquals(400, recorded.getFirst().attributes().get("http.response.status_code"));
    }

    private ThreadContext threadContext() {
        return new ThreadContext(Settings.EMPTY);
    }

    private RestRequest request(RestRequest.Method method, String path) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(method).withPath(path).build();
    }

    private RestResponse response(RestStatus status) {
        return new RestResponse(status, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
    }

    private List<Measurement> recordings() {
        return registry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SEARCH_DURATION_METRIC);
    }
}
