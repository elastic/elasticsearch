/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.Cluster.Status;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterQueryWithFiltersIT extends AbstractCrossClusterTestCase {
    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    protected void assertClusterMetadata(EsqlExecutionInfo.Cluster clusterMetatata, long took, String indexExpression, Status status) {
        assertThat(clusterMetatata.getIndexExpression(), equalTo(indexExpression));
        assertThat(clusterMetatata.getStatus(), equalTo(status));
        assertThat(clusterMetatata.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(clusterMetatata.getTook().millis(), lessThanOrEqualTo(took));
        assertThat(clusterMetatata.getFailedShards(), equalTo(0));
    }

    protected void assertClusterMetadataSuccess(EsqlExecutionInfo.Cluster clusterMetatata, int shards, long took, String indexExpression) {
        assertClusterMetadata(clusterMetatata, took, indexExpression, Status.SUCCESSFUL);
        assertThat(clusterMetatata.getTotalShards(), equalTo(shards));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(shards));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(0));
    }

    protected void assertClusterMetadataNoShards(EsqlExecutionInfo.Cluster clusterMetatata, long took, String indexExpression) {
        assertClusterMetadata(clusterMetatata, took, indexExpression, Status.SUCCESSFUL);
        assertThat(clusterMetatata.getTotalShards(), equalTo(0));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(0));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(0));
    }

    protected void assertClusterMetadataSkippedShards(
        EsqlExecutionInfo.Cluster clusterMetatata,
        int shards,
        long took,
        String indexExpression
    ) {
        assertClusterMetadata(clusterMetatata, took, indexExpression, Status.SUCCESSFUL);
        assertThat(clusterMetatata.getTotalShards(), equalTo(shards));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(shards));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(shards));
    }

    protected void assertClusterMetadataSkipped(EsqlExecutionInfo.Cluster clusterMetatata, long took, String indexExpression) {
        assertClusterMetadata(clusterMetatata, took, indexExpression, Status.SKIPPED);
        assertThat(clusterMetatata.getTotalShards(), equalTo(0));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(0));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(0));
    }

    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse, QueryBuilder filter) {
        EsqlQueryRequest request = randomBoolean() ? EsqlQueryRequest.asyncEsqlQueryRequest() : EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadataInResponse != null) {
            request.includeCCSMetadata(ccsMetadataInResponse);
        }
        if (filter != null) {
            request.filter(filter);
        }
        request.waitForCompletionTimeout(timeValueSeconds(30));
        return runQuery(request);
    }

    public void testTimestampFilterFromQuery() {
        int docsTest1 = 50;
        int docsTest2 = 30;
        int localShards = randomIntBetween(1, 5);
        int remoteShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        populateDateIndex(REMOTE_CLUSTER_1, REMOTE_INDEX, remoteShards, docsTest2, "2023-11-26");

        // Both indices are included
        var filter = new RangeQueryBuilder("@timestamp").from("2023-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-1,cluster-a:logs-2", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(docsTest1 + docsTest2));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataSuccess(remoteCluster, remoteShards, overallTookMillis, "logs-2");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-1");
        }

        // Only local is included
        filter = new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-1,cluster-a:logs-2", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(docsTest1));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-1");

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataSkippedShards(remoteCluster, remoteShards, overallTookMillis, "logs-2");
        }

        // Only remote is included
        filter = new RangeQueryBuilder("@timestamp").from("2023-01-01").to("2024-01-01");
        try (EsqlQueryResponse resp = runQuery("from logs-1,cluster-a:logs-2", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(docsTest2));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterMetadataSkippedShards(localCluster, localShards, overallTookMillis, "logs-1");

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataSuccess(remoteCluster, remoteShards, overallTookMillis, "logs-2");
        }

        // Only local is included - wildcards
        filter = new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-*", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(docsTest1));
            // FIXME: this is currently inconsistent with the non-wildcard case, since empty wildcard is not an error,
            // the second field-caps does not happen and the remote fields are not added to the response.
            // assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataNoShards(remoteCluster, overallTookMillis, "logs-*");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-*");
        }

        // Both indices are filtered out
        filter = new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-1,c*:logs-2", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(0));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // Remote has no shards due to filter
            assertClusterMetadataSkippedShards(remoteCluster, remoteShards, overallTookMillis, "logs-2");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            // Local cluster can not be filtered out for now
            assertClusterMetadataSkippedShards(localCluster, localShards, overallTookMillis, "logs-1");
        }

        // Both indices are filtered out - wildcards
        filter = new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-*", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(0));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // Remote has no shards due to filter
            assertClusterMetadataSkippedShards(remoteCluster, remoteShards, overallTookMillis, "logs-*");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            // Local cluster can not be filtered out for now
            assertClusterMetadataSkippedShards(localCluster, localShards, overallTookMillis, "logs-*");
        }

    }

    public void testFilterWithMissingIndex() {
        int docsTest1 = 50;
        int docsTest2 = 30;
        int localShards = randomIntBetween(1, 5);
        int remoteShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        populateDateIndex(REMOTE_CLUSTER_1, REMOTE_INDEX, remoteShards, docsTest2, "2023-11-26");

        int count = 0;
        for (var filter : List.of(
            new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now")
        )) {
            count++;
            // Local index missing
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> runQuery("from missing", randomBoolean(), filter).close()
            );
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Local index missing + wildcards
            // FIXME: planner does not catch this now, it should be VerificationException but for now it's runtime IndexNotFoundException
            var ie = expectThrows(IndexNotFoundException.class, () -> runQuery("from missing,logs*", randomBoolean(), filter).close());
            assertThat(ie.getDetailedMessage(), containsString("no such index [missing]"));
            // Local index missing + existing index
            // FIXME: planner does not catch this now, it should be VerificationException but for now it's runtime IndexNotFoundException
            ie = expectThrows(IndexNotFoundException.class, () -> runQuery("from missing,logs-1", randomBoolean(), filter).close());
            assertThat(ie.getDetailedMessage(), containsString("no such index [missing]"));
            // Local index missing + existing remote
            e = expectThrows(VerificationException.class, () -> runQuery("from missing,cluster-a:logs-2", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Wildcard index missing
            e = expectThrows(VerificationException.class, () -> runQuery("from missing*", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing*]"));
            // Wildcard index missing + existing index
            try (EsqlQueryResponse resp = runQuery("from missing*,logs-1", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(count > 1 ? 0 : docsTest1));
            }
        }
    }

    public void testFilterWithMissingRemoteIndex() {
        int docsTest1 = 50;
        int docsTest2 = 30;
        int localShards = randomIntBetween(1, 5);
        int remoteShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        populateDateIndex(REMOTE_CLUSTER_1, REMOTE_INDEX, remoteShards, docsTest2, "2023-11-26");

        int count = 0;
        for (var filter : List.of(
            new RangeQueryBuilder("@timestamp").from("2023-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now")
        )) {
            count++;
            // Local index missing
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> runQuery("from cluster-a:missing", randomBoolean(), filter).close()
            );
            assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:missing]"));
            // Local index missing + wildcards
            // FIXME: planner does not catch this now, it should be VerificationException but for now it's runtime RemoteException
            var ie = expectThrows(
                RemoteException.class,
                () -> runQuery("from cluster-a:missing,cluster-a:logs*", randomBoolean(), filter).close()
            );
            assertThat(ie.getDetailedMessage(), containsString("no such index [missing]"));
            // Local index missing + existing index
            // FIXME: planner does not catch this now, it should be VerificationException but for now it's runtime RemoteException
            ie = expectThrows(
                RemoteException.class,
                () -> runQuery("from cluster-a:missing,cluster-a:logs-2", randomBoolean(), filter).close()
            );
            assertThat(ie.getDetailedMessage(), containsString("no such index [missing]"));
            // Local index + missing remote
            e = expectThrows(VerificationException.class, () -> runQuery("from logs-1,cluster-a:missing", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:missing]"));
            // Wildcard index missing
            e = expectThrows(VerificationException.class, () -> runQuery("from cluster-a:missing*", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:missing*]"));
            // Wildcard index missing + existing remote index
            try (EsqlQueryResponse resp = runQuery("from cluster-a:missing*,cluster-a:logs-2", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(count > 1 ? 0 : docsTest2));
            }
            // Wildcard index missing + existing local index
            try (EsqlQueryResponse resp = runQuery("from cluster-a:missing*,logs-1", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(count > 2 ? 0 : docsTest1));
            }
        }
    }

    private void checkRemoteFailures() {
        for (var filter : List.of(
            new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now")
        )) {
            // One index
            var e = expectThrows(ElasticsearchException.class, () -> runQuery("from cluster-a:log-2", randomBoolean(), filter).close());
            // Two indices
            e = expectThrows(ElasticsearchException.class, () -> runQuery("from logs-1,cluster-a:log-2", randomBoolean(), filter).close());
            // Wildcard
            e = expectThrows(ElasticsearchException.class, () -> runQuery("from logs-1,cluster-a:log*", randomBoolean(), filter).close());
        }
    }

    private void checkRemoteWithSkipUnavailable() {
        int count = 0;
        int docsTest1 = 50;
        int localShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");

        for (var filter : List.of(
            new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now")
        )) {
            count++;
            // One index
            try (EsqlQueryResponse resp = runQuery("from cluster-a:logs-2", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterMetadataSkipped(remoteCluster, overallTookMillis, "logs-2");
            }
            // Two indices
            try (EsqlQueryResponse resp = runQuery("from logs-1,cluster-a:logs-2", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(count > 1 ? 0 : docsTest1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterMetadataSkipped(remoteCluster, overallTookMillis, "logs-2");

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                if (count > 1) {
                    assertClusterMetadataNoShards(localCluster, overallTookMillis, "logs-1");
                } else {
                    assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-1");
                }
            }
            // Wildcard
            try (EsqlQueryResponse resp = runQuery("from logs-1,cluster-a:logs*", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(count > 1 ? 0 : docsTest1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterMetadataSkipped(remoteCluster, overallTookMillis, "logs*");

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                if (count > 1) {
                    assertClusterMetadataNoShards(localCluster, overallTookMillis, "logs-1");
                } else {
                    assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-1");
                }
            }
        }
    }

    public void testFilterWithUnavailableRemote() throws IOException {
        int docsTest1 = 50;
        int localShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        cluster(REMOTE_CLUSTER_1).close();
        checkRemoteFailures();
    }

    private void makeRemoteFailFieldCaps() {
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.addRequestHandlingBehavior(
                EsqlResolveFieldsAction.NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        sendResponse(new NoSuchRemoteClusterException("cluster [cluster-a] not found, skipping"));
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task)
            );
        }
    }

    private void clearRemoteRules() {
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            MockTransportService ts = asInstanceOf(MockTransportService.class, transportService);
            ts.clearAllRules();
        }
    }

    // Test when the disconnect happens on the field-caps call itself
    public void testFilterWithUnavailableOnFieldcaps() throws IOException {
        int docsTest1 = 50;
        int localShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        makeRemoteFailFieldCaps();
        try {
            checkRemoteFailures();
        } finally {
            clearRemoteRules();
        }
    }

    public void testFilterWithUnavailableRemoteAndSkipUnavailable() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        cluster(REMOTE_CLUSTER_1).close();
        checkRemoteWithSkipUnavailable();
    }

    public void testFilterWithUnavailableFieldCapsAndSkipUnavailable() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        makeRemoteFailFieldCaps();
        try {
            checkRemoteWithSkipUnavailable();
        } finally {
            clearRemoteRules();
        }
    }

    protected void populateDateIndex(String clusterAlias, String indexName, int numShards, int numDocs, String date) {
        Client client = client(clusterAlias);
        String tag = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping(
                    "id",
                    "type=keyword",
                    "tag-" + tag,
                    "type=keyword",
                    "v",
                    "type=long",
                    "const",
                    "type=long",
                    "@timestamp",
                    "type=date"
                )
        );
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Long.toString(i);
            client.prepareIndex(indexName).setSource("id", id, "tag-" + tag, tag, "v", i, "@timestamp", date).get();
        }
        client.admin().indices().prepareRefresh(indexName).get();
    }

}
