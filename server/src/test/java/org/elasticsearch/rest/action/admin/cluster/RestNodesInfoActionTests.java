/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction.ALLOWED_METRICS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class RestNodesInfoActionTests extends ESTestCase {

    public void testDuplicatedFiltersAreNotRemoved() {
        Map<String, String> params = new HashMap<>();
        params.put("nodeId", "_all,master:false,_all");

        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all", "master:false", "_all" }, actual.nodesIds());
    }

    public void testOnlyMetrics() {
        Map<String, String> params = new HashMap<>();
        int metricsCount = randomIntBetween(1, ALLOWED_METRICS.size());
        List<String> metrics = new ArrayList<>();

        for (int i = 0; i < metricsCount; i++) {
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        params.put("nodeId", String.join(",", metrics));

        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all" }, actual.nodesIds());
        assertMetrics(metrics, actual);
    }

    public void testAllMetricsSelectedWhenNodeAndMetricSpecified() {
        Map<String, String> params = new HashMap<>();
        String nodeId = randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23));
        String metric = randomFrom(ALLOWED_METRICS);

        params.put("nodeId", nodeId + "," + metric);
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { nodeId, metric }, actual.nodesIds());
        assertAllMetricsTrue(actual);
    }

    public void testSeparateNodeIdsAndMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);
        List<String> metrics = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
            metrics.add(randomFrom(ALLOWED_METRICS));
        }

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", String.join(",", metrics));
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertMetrics(metrics, actual);
    }

    public void testExplicitAllMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
        }

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", "_all");
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertAllMetricsTrue(actual);
    }

    /**
     * Test that if a user requests a non-existent metric, it's dropped from the
     * request without an error.
     */
    public void testNonexistentMetricsDropped() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);
        List<String> metrics = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        String nonAllowedMetric = randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(5));
        metrics.add(nonAllowedMetric);

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", String.join(",", metrics));
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertThat(actual.requestedMetrics(), not(hasItem(nonAllowedMetric)));
        assertMetrics(metrics, actual);
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_nodes")
            .withParams(params)
            .build();
    }

    private void assertMetrics(List<String> metrics, NodesInfoRequest nodesInfoRequest) {
        Set<String> validRequestedMetrics = Sets.intersection(new HashSet<>(metrics), ALLOWED_METRICS);
        assertThat(nodesInfoRequest.requestedMetrics(), equalTo(validRequestedMetrics));
    }

    private void assertAllMetricsTrue(NodesInfoRequest nodesInfoRequest) {
        assertThat(nodesInfoRequest.requestedMetrics(), equalTo(ALLOWED_METRICS));
    }
}
