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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class RestRequestMetricsTests extends ESTestCase {

    private static final String SEARCH_DURATION_METRIC = "es.rest.search.duration";
    private static final String MSEARCH_DURATION_METRIC = "es.rest.msearch.duration";
    private static final String ESQL_DURATION_METRIC = "es.rest.esql.duration";
    private static final String INDEX_DURATION_METRIC = "es.rest.index.duration";
    private static final String UPDATE_DURATION_METRIC = "es.rest.update.duration";
    private static final String BULK_DURATION_METRIC = "es.rest.bulk.duration";
    private static final String COUNT_DURATION_METRIC = "es.rest.count.duration";
    private static final String SEARCH_SCROLL_DURATION_METRIC = "es.rest.search_scroll.duration";
    private static final String PROMETHEUS_QUERY_DURATION_METRIC = "es.rest.prometheus_query.duration";
    private static final String PROMETHEUS_WRITE_DURATION_METRIC = "es.rest.prometheus_write.duration";

    private final RecordingMeterRegistry registry = new RecordingMeterRegistry();
    private final RestRequestMetrics metrics = new RestRequestMetrics(registry);

    public void test_measuredRoutes_recordDuration() {
        record Case(RestRequest.Method method, String path, String route, String metric) {}

        var cases = List.of(
            // search
            new Case(RestRequest.Method.GET, "/_search", "_search", SEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_search", "_search", SEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.GET, "/my-index/_search", "{index}/_search", SEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_search", "{index}/_search", SEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.GET, "/my-index/_knn_search", "{index}/_knn_search", SEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_knn_search", "{index}/_knn_search", SEARCH_DURATION_METRIC),

            // multi search
            new Case(RestRequest.Method.GET, "/_msearch", "_msearch", MSEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_msearch", "_msearch", MSEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.GET, "/my-index/_msearch", "{index}/_msearch", MSEARCH_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_msearch", "{index}/_msearch", MSEARCH_DURATION_METRIC),

            // ESQL sync query
            new Case(RestRequest.Method.POST, "/_query", "_query", ESQL_DURATION_METRIC),

            // index
            new Case(RestRequest.Method.POST, "/my-index/_doc/1", "{index}/_doc/{id}", INDEX_DURATION_METRIC),
            new Case(RestRequest.Method.PUT, "/my-index/_doc/1", "{index}/_doc/{id}", INDEX_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_create/1", "{index}/_create/{id}", INDEX_DURATION_METRIC),
            new Case(RestRequest.Method.PUT, "/my-index/_create/1", "{index}/_create/{id}", INDEX_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_doc", "{index}/_doc", INDEX_DURATION_METRIC),

            // update
            new Case(RestRequest.Method.POST, "/my-index/_update/1", "{index}/_update/{id}", UPDATE_DURATION_METRIC),

            // bulk
            new Case(RestRequest.Method.POST, "/_bulk", "_bulk", BULK_DURATION_METRIC),
            new Case(RestRequest.Method.PUT, "/_bulk", "_bulk", BULK_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_bulk", "{index}/_bulk", BULK_DURATION_METRIC),
            new Case(RestRequest.Method.PUT, "/my-index/_bulk", "{index}/_bulk", BULK_DURATION_METRIC),

            // count
            new Case(RestRequest.Method.GET, "/_count", "_count", COUNT_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_count", "_count", COUNT_DURATION_METRIC),
            new Case(RestRequest.Method.GET, "/my-index/_count", "{index}/_count", COUNT_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/my-index/_count", "{index}/_count", COUNT_DURATION_METRIC),

            // search scroll
            new Case(RestRequest.Method.GET, "/_search/scroll", "_search/scroll", SEARCH_SCROLL_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_search/scroll", "_search/scroll", SEARCH_SCROLL_DURATION_METRIC),
            new Case(RestRequest.Method.GET, "/_search/scroll/abc123", "_search/scroll/{scroll_id}", SEARCH_SCROLL_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_search/scroll/abc123", "_search/scroll/{scroll_id}", SEARCH_SCROLL_DURATION_METRIC),

            // prometheus query
            new Case(RestRequest.Method.GET, "/_prometheus/api/v1/series", "/_prometheus/api/v1/series", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_prometheus/api/v1/series", "/_prometheus/api/v1/series", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/series",
                "/_prometheus/{index}/api/v1/series",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/my-index/api/v1/series",
                "/_prometheus/{index}/api/v1/series",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/api/v1/query_range",
                "/_prometheus/api/v1/query_range",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/api/v1/query_range",
                "/_prometheus/api/v1/query_range",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/query_range",
                "/_prometheus/{index}/api/v1/query_range",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/my-index/api/v1/query_range",
                "/_prometheus/{index}/api/v1/query_range",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(RestRequest.Method.GET, "/_prometheus/api/v1/query", "/_prometheus/api/v1/query", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_prometheus/api/v1/query", "/_prometheus/api/v1/query", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/query",
                "/_prometheus/{index}/api/v1/query",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/my-index/api/v1/query",
                "/_prometheus/{index}/api/v1/query",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(RestRequest.Method.GET, "/_prometheus/api/v1/labels", "/_prometheus/api/v1/labels", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(RestRequest.Method.POST, "/_prometheus/api/v1/labels", "/_prometheus/api/v1/labels", PROMETHEUS_QUERY_DURATION_METRIC),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/labels",
                "/_prometheus/{index}/api/v1/labels",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/my-index/api/v1/labels",
                "/_prometheus/{index}/api/v1/labels",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/api/v1/label/foo/values",
                "/_prometheus/api/v1/label/{name}/values",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/label/foo/values",
                "/_prometheus/{index}/api/v1/label/{name}/values",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/api/v1/metadata",
                "/_prometheus/api/v1/metadata",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.GET,
                "/_prometheus/my-index/api/v1/metadata",
                "/_prometheus/{index}/api/v1/metadata",
                PROMETHEUS_QUERY_DURATION_METRIC
            ),

            // prometheus write
            new Case(RestRequest.Method.POST, "/_prometheus/api/v1/write", "/_prometheus/api/v1/write", PROMETHEUS_WRITE_DURATION_METRIC),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/metrics/my-dataset/api/v1/write",
                "/_prometheus/metrics/{dataset}/api/v1/write",
                PROMETHEUS_WRITE_DURATION_METRIC
            ),
            new Case(
                RestRequest.Method.POST,
                "/_prometheus/metrics/my-dataset/my-namespace/api/v1/write",
                "/_prometheus/metrics/{dataset}/{namespace}/api/v1/write",
                PROMETHEUS_WRITE_DURATION_METRIC
            )
        );

        for (var c : cases) {
            var ctx = threadContext();
            var request = request(c.method, c.path);
            metrics.start(ctx, request, c.route);
            metrics.prepareEnd(ctx, request, response(RestStatus.OK)).close();
        }

        var expectedCountsByMetric = cases.stream().collect(Collectors.groupingBy(Case::metric, Collectors.counting()));
        for (var entry : expectedCountsByMetric.entrySet()) {
            String metric = entry.getKey();
            long count = entry.getValue();
            List<Measurement> measurements = recordings(metric);

            assertEquals(metric, count, measurements.size());
            for (var m : measurements) {
                assertThat(m.getDouble(), greaterThanOrEqualTo(0.0));
            }
        }
    }

    public void test_leadingSlashStripped() {
        var ctx = threadContext();
        var request = request(RestRequest.Method.GET, "/_search");

        metrics.start(ctx, request, "/_search");
        var end = metrics.prepareEnd(ctx, request, response(RestStatus.OK));

        assertThat(recordings(SEARCH_DURATION_METRIC), empty());

        end.close();

        assertThat(recordings(SEARCH_DURATION_METRIC), hasSize(1));
    }

    public void test_nullRoute_noRecording() {
        var ctx = threadContext();
        var request = request(RestRequest.Method.GET, "/_search");

        metrics.start(ctx, request, null);
        metrics.prepareEnd(ctx, request, response(RestStatus.OK)).close();

        assertThat(recordings(SEARCH_DURATION_METRIC), empty());
    }

    public void test_unmeasuredRoute_noRecording() {
        var ctx = threadContext();
        var request = request(RestRequest.Method.GET, "/_cluster/health");

        metrics.start(ctx, request, "_cluster/health");
        metrics.prepareEnd(ctx, request, response(RestStatus.OK)).close();

        assertThat(registry.getRecorder().getAllMeasurements(), empty());
    }

    public void test_prepareEndWithoutStart_noRecording() {
        var ctx = threadContext();
        var request = request(RestRequest.Method.GET, "/_search");

        metrics.prepareEnd(ctx, request, response(RestStatus.OK)).close();

        assertThat(recordings(SEARCH_DURATION_METRIC), empty());
    }

    public void test_statusCodeAttribute() {
        var ctx = threadContext();
        var request = request(RestRequest.Method.GET, "/_search");

        metrics.start(ctx, request, "_search");
        var end = metrics.prepareEnd(ctx, request, response(RestStatus.BAD_REQUEST));

        assertThat(recordings(SEARCH_DURATION_METRIC), empty());

        end.close();

        var recorded = recordings(SEARCH_DURATION_METRIC);
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

    private List<Measurement> recordings(String metric) {
        return registry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, metric);
    }
}
