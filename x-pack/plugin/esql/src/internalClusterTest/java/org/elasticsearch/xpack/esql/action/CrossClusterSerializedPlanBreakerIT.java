/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.KEY_PADDING;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.NUM_DOCS;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.QUERY_TAIL;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.assertTrippedOn;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.fillBreakerOnOpenExchange;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

/**
 * The coordinator charges its request breaker for the sub-plan result it serializes into the request to each remote cluster.
 * The breaker is filled after the first remote cluster's reservation so only the second one trips.
 */
public class CrossClusterSerializedPlanBreakerIT extends AbstractCrossClusterTestCase {

    private static final String INDEX = "breaker_idx";
    private static final String QUERY = "FROM " + REMOTE_CLUSTER_1 + ":" + INDEX + "," + REMOTE_CLUSTER_2 + ":" + INDEX + QUERY_TAIL;

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(SerializedPlanBreakerIT.memoryRequestBreaker()).build();
    }

    public void testTripFailsQueryWithoutPartialResults() {
        populateRemotes();
        String coordinator = cluster(LOCAL_CLUSTER).getRandomNodeName();
        try (Releasable filled = fillCoordinatorBreaker(coordinator)) {
            Exception e = expectThrows(Exception.class, () -> run(coordinator, false).close());
            assertTrippedOn(e, "serialize cluster compute request");
        }
    }

    public void testTripDropsClusterWithPartialResults() {
        populateRemotes();
        String coordinator = cluster(LOCAL_CLUSTER).getRandomNodeName();
        try (Releasable filled = fillCoordinatorBreaker(coordinator); EsqlQueryResponse resp = run(coordinator, true)) {
            assertTrue(resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(((Number) rows.get(0).get(0)).longValue(), lessThan((long) NUM_DOCS));
            StringBuilder failures = new StringBuilder();
            for (String alias : List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)) {
                failures.append(resp.getExecutionInfo().getCluster(alias).getFailures());
            }
            assertThat(failures.toString(), containsString("data for [serialize cluster compute request]"));
        }
    }

    private void populateRemotes() {
        for (String alias : List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)) {
            Client remote = client(alias);
            assertAcked(remote.admin().indices().prepareCreate(INDEX).setMapping("k", "type=keyword"));
            BulkRequestBuilder bulk = remote.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < NUM_DOCS / 2; i++) {
                bulk.add(remote.prepareIndex(INDEX).setSource("k", alias + i + KEY_PADDING));
            }
            assertFalse(bulk.get().hasFailures());
        }
    }

    private Releasable fillCoordinatorBreaker(String coordinator) {
        return fillBreakerOnOpenExchange(
            (MockTransportService) cluster(LOCAL_CLUSTER).getInstance(TransportService.class, coordinator),
            cluster(LOCAL_CLUSTER).getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST),
            3
        );
    }

    private EsqlQueryResponse run(String coordinator, boolean allowPartialResults) {
        var request = syncEsqlQueryRequest(QUERY).allowPartialResults(allowPartialResults);
        return cluster(LOCAL_CLUSTER).client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT);
    }
}
