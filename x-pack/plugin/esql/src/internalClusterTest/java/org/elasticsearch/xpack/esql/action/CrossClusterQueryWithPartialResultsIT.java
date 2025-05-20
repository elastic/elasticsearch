/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CrossClusterQueryWithPartialResultsIT extends AbstractCrossClusterTestCase {

    private static class ClusterSetup {
        final int okShards = randomIntBetween(1, 5);
        final int failingShards = randomIntBetween(1, 5);
        Set<String> okIds;
    }

    private final ClusterSetup local = new ClusterSetup();
    private final ClusterSetup remote1 = new ClusterSetup();
    private final ClusterSetup remote2 = new ClusterSetup();

    void populateIndices() throws Exception {
        local.okIds = populateIndex(LOCAL_CLUSTER, "ok-local", local.okShards, between(1, 100));
        populateIndexWithFailingFields(LOCAL_CLUSTER, "fail-local", local.failingShards);

        remote1.okIds = populateIndex(REMOTE_CLUSTER_1, "ok-cluster1", remote1.okShards, between(1, 100));
        populateIndexWithFailingFields(REMOTE_CLUSTER_1, "fail-cluster1", remote1.failingShards);

        remote2.okIds = populateIndex(REMOTE_CLUSTER_2, "ok-cluster2", remote2.okShards, between(1, 100));
        populateIndexWithFailingFields(REMOTE_CLUSTER_2, "fail-cluster2", remote2.failingShards);
    }

    private void assertClusterPartial(EsqlQueryResponse resp, String clusterAlias, ClusterSetup cluster) {
        assertClusterPartial(resp, clusterAlias, cluster.okShards + cluster.failingShards, cluster.okShards);
    }

    private void assertClusterFailure(EsqlQueryResponse resp, String clusterAlias, String reason) {
        EsqlExecutionInfo.Cluster info = resp.getExecutionInfo().getCluster(clusterAlias);
        assertThat(info.getFailures(), not(empty()));
        for (ShardSearchFailure f : info.getFailures()) {
            assertThat(f.reason(), containsString(reason));
        }
    }

    private void assertClusterPartial(EsqlQueryResponse resp, String clusterAlias, int totalShards, int okShards) {
        EsqlExecutionInfo.Cluster clusterInfo = resp.getExecutionInfo().getCluster(clusterAlias);
        assertThat(clusterInfo.getTotalShards(), equalTo(totalShards));
        assertThat(clusterInfo.getSuccessfulShards(), lessThanOrEqualTo(okShards));
        assertThat(clusterInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    private void assertClusterSuccess(EsqlQueryResponse resp, String clusterAlias, int numShards) {
        EsqlExecutionInfo.Cluster clusterInfo = resp.getExecutionInfo().getCluster(clusterAlias);
        assertThat(clusterInfo.getTotalShards(), equalTo(numShards));
        assertThat(clusterInfo.getSuccessfulShards(), equalTo(numShards));
        assertThat(clusterInfo.getFailedShards(), equalTo(0));
        assertThat(clusterInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(clusterInfo.getFailures(), empty());
    }

    public void testPartialResults() throws Exception {
        populateIndices();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM ok*,fail*,*:ok*,*:fail* | KEEP id, fail_me | LIMIT 1000");
        request.includeCCSMetadata(randomBoolean());
        {
            request.allowPartialResults(false);
            Exception error = expectThrows(Exception.class, () -> runQuery(request).close());
            error = EsqlTestUtils.unwrapIfWrappedInRemoteException(error);

            assertThat(error, instanceOf(IllegalStateException.class));
            assertThat(error.getMessage(), containsString("Accessing failing field"));
        }
        request.allowPartialResults(true);
        try (var resp = runQuery(request)) {
            assertTrue(resp.isPartial());
            Set<String> allIds = Stream.of(local.okIds, remote1.okIds, remote2.okIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows.size(), lessThanOrEqualTo(allIds.size()));
            Set<String> returnedIds = new HashSet<>();
            for (List<Object> row : rows) {
                assertThat(row.size(), equalTo(2));
                String id = (String) row.get(0);
                assertTrue(returnedIds.add(id));
                assertThat(id, is(in(allIds)));
            }
            assertClusterPartial(resp, LOCAL_CLUSTER, local);
            assertClusterPartial(resp, REMOTE_CLUSTER_1, remote1);
            assertClusterPartial(resp, REMOTE_CLUSTER_2, remote2);
            for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)) {
                assertClusterFailure(resp, cluster, "Accessing failing field");
            }
        }
    }

    public void testOneRemoteClusterPartial() throws Exception {
        populateIndices();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM ok*,cluster-a:ok*,*-b:fail* | KEEP id, fail_me");
        request.allowPartialResults(true);
        request.includeCCSMetadata(randomBoolean());
        try (var resp = runQuery(request)) {
            assertTrue(resp.isPartial());
            Set<String> allIds = Stream.of(local.okIds, remote1.okIds).flatMap(Collection::stream).collect(Collectors.toSet());
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows.size(), equalTo(allIds.size()));
            Set<String> returnedIds = new HashSet<>();
            for (List<Object> row : rows) {
                assertThat(row.size(), equalTo(2));
                String id = (String) row.get(0);
                assertTrue(returnedIds.add(id));
            }
            assertThat(returnedIds, equalTo(allIds));

            assertClusterSuccess(resp, LOCAL_CLUSTER, local.okShards);
            assertClusterSuccess(resp, REMOTE_CLUSTER_1, remote1.okShards);
            assertClusterPartial(resp, REMOTE_CLUSTER_2, remote2.failingShards, 0);
            assertClusterFailure(resp, REMOTE_CLUSTER_2, "Accessing failing field");
        }
    }

    public void testLocalIndexMissing() throws Exception {
        populateIndices();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM ok-local,no_such_index | LIMIT 1");
        request.includeCCSMetadata(randomBoolean());
        for (boolean allowPartial : Set.of(true, false)) {
            request.allowPartialResults(allowPartial);
            Exception error = expectThrows(Exception.class, () -> runQuery(request).close());
            error = EsqlTestUtils.unwrapIfWrappedInRemoteException(error);
            assertThat(error.getMessage(), containsString("no such index"));
            assertThat(error.getMessage(), containsString("[no_such_index]"));
        }
    }

    public void testRemoteIndexMissing() throws Exception {
        populateIndices();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM cluster-a:ok-cluster1,cluster-a:no_such_index | LIMIT 1");
        request.includeCCSMetadata(randomBoolean());
        for (boolean allowPartial : Set.of(true, false)) {
            request.allowPartialResults(allowPartial);
            Exception error = expectThrows(Exception.class, () -> runQuery(request).close());
            error = EsqlTestUtils.unwrapIfWrappedInRemoteException(error);
            assertThat(error.getMessage(), containsString("no such index"));
            assertThat(error.getMessage(), containsString("[no_such_index]"));
        }
    }

    public void testFailToReceiveClusterResponse() throws Exception {
        populateIndices();
        Exception simulatedFailure = randomFailure();
        // fetched pages, but failed to receive the cluster response
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.addRequestHandlingBehavior(
                ComputeService.CLUSTER_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        sendResponse(simulatedFailure);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task)
            );
        }
        try {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM ok*,cluster-a:ok* | KEEP id");
            request.includeCCSMetadata(randomBoolean());
            {
                request.allowPartialResults(false);
                Exception error = expectThrows(Exception.class, () -> runQuery(request).close());
                error = EsqlTestUtils.unwrapIfWrappedInRemoteException(error);
                var unwrapped = ExceptionsHelper.unwrap(error, simulatedFailure.getClass());
                assertNotNull(unwrapped);
                assertThat(unwrapped.getMessage(), equalTo(simulatedFailure.getMessage()));
            }
            request.allowPartialResults(true);
            try (var resp = runQuery(request)) {
                assertTrue(resp.isPartial());
                List<List<Object>> rows = getValuesList(resp);
                Set<String> returnedIds = new HashSet<>();
                for (List<Object> row : rows) {
                    assertThat(row.size(), equalTo(1));
                    String id = (String) row.get(0);
                    assertTrue(returnedIds.add(id));
                }
                assertThat(returnedIds, equalTo(Sets.union(local.okIds, remote1.okIds)));
                assertClusterSuccess(resp, LOCAL_CLUSTER, local.okShards);
                EsqlExecutionInfo.Cluster remoteInfo = resp.getExecutionInfo().getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
                assertClusterFailure(resp, REMOTE_CLUSTER_1, simulatedFailure.getMessage());
            }
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    public void testFailToStartRequestOnRemoteCluster() throws Exception {
        populateIndices();
        Exception simulatedFailure = randomFailure();
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            String actionToFail = randomFrom(
                ExchangeService.EXCHANGE_ACTION_NAME,
                ExchangeService.OPEN_EXCHANGE_ACTION_NAME,
                ComputeService.CLUSTER_ACTION_NAME
            );
            ts.addRequestHandlingBehavior(actionToFail, (handler, request, channel, task) -> { channel.sendResponse(simulatedFailure); });
        }
        try {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM ok*,*a:ok* | KEEP id");
            request.includeCCSMetadata(randomBoolean());
            {
                request.allowPartialResults(false);
                Exception error = expectThrows(Exception.class, () -> runQuery(request).close());
                error = EsqlTestUtils.unwrapIfWrappedInRemoteException(error);
                EsqlTestUtils.assertEsqlFailure(error);
                var unwrapped = ExceptionsHelper.unwrap(error, simulatedFailure.getClass());
                assertNotNull(unwrapped);
                assertThat(unwrapped.getMessage(), equalTo(simulatedFailure.getMessage()));
            }
            request.allowPartialResults(true);
            try (var resp = runQuery(request)) {
                assertTrue(resp.isPartial());
                List<List<Object>> rows = getValuesList(resp);
                Set<String> returnedIds = new HashSet<>();
                for (List<Object> row : rows) {
                    assertThat(row.size(), equalTo(1));
                    String id = (String) row.get(0);
                    assertTrue(returnedIds.add(id));
                }
                assertThat(returnedIds, equalTo(local.okIds));
                assertClusterSuccess(resp, LOCAL_CLUSTER, local.okShards);
                EsqlExecutionInfo.Cluster remoteInfo = resp.getExecutionInfo().getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
                assertClusterFailure(resp, REMOTE_CLUSTER_1, simulatedFailure.getMessage());
            }
        } finally {
            for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    public void testFailSearchShardsOnLocalCluster() throws Exception {
        populateIndices();
        Exception simulatedFailure = randomFailure();
        for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.addRequestHandlingBehavior(
                EsqlSearchShardsAction.NAME,
                (handler, request, channel, task) -> { channel.sendResponse(simulatedFailure); }
            );
        }
        try {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM ok*,*a:ok* | KEEP id");
            request.includeCCSMetadata(randomBoolean());
            {
                request.allowPartialResults(false);
                var error = expectThrows(Exception.class, () -> runQuery(request).close());
                EsqlTestUtils.assertEsqlFailure(error);
                var unwrapped = ExceptionsHelper.unwrap(error, simulatedFailure.getClass());
                assertNotNull(unwrapped);
                assertThat(unwrapped.getMessage(), equalTo(simulatedFailure.getMessage()));
            }
            request.allowPartialResults(true);
            try (var resp = runQuery(request)) {
                assertTrue(resp.isPartial());
                List<List<Object>> rows = getValuesList(resp);
                Set<String> returnedIds = new HashSet<>();
                for (List<Object> row : rows) {
                    assertThat(row.size(), equalTo(1));
                    String id = (String) row.get(0);
                    assertTrue(returnedIds.add(id));
                }
                assertThat(returnedIds, equalTo(remote1.okIds));
                EsqlExecutionInfo.Cluster localInfo = resp.getExecutionInfo().getCluster(LOCAL_CLUSTER);
                assertThat(localInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
                assertClusterFailure(resp, LOCAL_CLUSTER, simulatedFailure.getMessage());
            }
        } finally {
            for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
                MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
                ts.clearAllRules();
            }
        }
    }

    private static Exception randomFailure() {
        return randomFrom(
            new IllegalStateException("driver was closed already"),
            new CircuitBreakingException("low memory", CircuitBreaker.Durability.PERMANENT),
            new IOException("broken disk"),
            new ResourceNotFoundException("exchange sink was not found"),
            new EsRejectedExecutionException("node is shutting down")
        );
    }

    private Set<String> populateIndexWithFailingFields(String clusterAlias, String indexName, int numShards) throws IOException {
        Client client = client(clusterAlias);
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", FailingFieldPlugin.FAILING_FIELD_LANG).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
            mapping.startObject("id").field("type", "keyword").endObject();
            mapping.startObject("tag").field("type", "keyword").endObject();
        }
        mapping.endObject();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping(mapping.endObject())
        );
        Set<String> ids = new HashSet<>();
        String tag = clusterAlias.isEmpty() ? "local" : clusterAlias;
        int numDocs = between(50, 100); // large enough to have failing documents in every shard
        for (int i = 0; i < numDocs; i++) {
            String id = Long.toString(NEXT_DOC_ID.incrementAndGet());
            client.prepareIndex(indexName).setSource("id", id, "tag", tag, "v", i).get();
            ids.add(id);
        }
        client.admin().indices().prepareRefresh(indexName).get();
        for (var shardStats : client.admin().indices().prepareStats(indexName).clear().setDocs(true).get().getShards()) {
            var docsStats = shardStats.getStats().docs;
            assertNotNull(docsStats);
            assertThat("no doc for shard " + shardStats.getShardRouting().shardId(), docsStats.getCount(), greaterThan(0L));
        }
        return ids;
    }
}
