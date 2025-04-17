/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TestLogging(value = "org.elasticsearch.xpack.esql:DEBUG", reason = "debug")
public class CrossClusterQueryWithFiltersIT extends AbstractCrossClusterTestCase {
    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    protected void assertClusterMetadataSuccess(EsqlExecutionInfo.Cluster clusterMetatata, int shards, long took, String indexExpression) {
        assertThat(clusterMetatata.getIndexExpression(), equalTo(indexExpression));
        assertThat(clusterMetatata.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(clusterMetatata.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(clusterMetatata.getTook().millis(), lessThanOrEqualTo(took));
        assertThat(clusterMetatata.getTotalShards(), equalTo(shards));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(shards));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(0));
        assertThat(clusterMetatata.getFailedShards(), equalTo(0));
    }

    protected void assertClusterMetadataNoShards(EsqlExecutionInfo.Cluster clusterMetatata, int shards, long took, String indexExpression) {
        assertThat(clusterMetatata.getIndexExpression(), equalTo(indexExpression));
        assertThat(clusterMetatata.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(clusterMetatata.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(clusterMetatata.getTook().millis(), lessThanOrEqualTo(took));
        assertThat(clusterMetatata.getTotalShards(), equalTo(0));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(0));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(0));
        assertThat(clusterMetatata.getFailedShards(), equalTo(0));
    }

    protected void assertClusterMetadataSkipped(EsqlExecutionInfo.Cluster clusterMetatata, int shards, long took, String indexExpression) {
        assertThat(clusterMetatata.getIndexExpression(), equalTo(indexExpression));
        assertThat(clusterMetatata.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(clusterMetatata.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(clusterMetatata.getTook().millis(), lessThanOrEqualTo(took));
        assertThat(clusterMetatata.getTotalShards(), equalTo(shards));
        assertThat(clusterMetatata.getSuccessfulShards(), equalTo(shards));
        assertThat(clusterMetatata.getSkippedShards(), equalTo(shards));
        assertThat(clusterMetatata.getFailedShards(), equalTo(0));
    }

    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse, QueryBuilder filter) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
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
            assertClusterMetadataNoShards(remoteCluster, remoteShards, overallTookMillis, "logs-2");
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
            assertClusterMetadataSkipped(localCluster, localShards, overallTookMillis, "logs-1");

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataSuccess(remoteCluster, remoteShards, overallTookMillis, "logs-2");
        }

        // Only local is included - wildcards
        filter = new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-*", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(docsTest1));
            // assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterMetadataNoShards(remoteCluster, remoteShards, overallTookMillis, "logs-*");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterMetadataSuccess(localCluster, localShards, overallTookMillis, "logs-*");
        }

        // Both indices are filtered out
        filter = new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-1,c*:logs-2", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(0));
            // assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // Remote has no shards due to filter
            assertClusterMetadataNoShards(remoteCluster, remoteShards, overallTookMillis, "logs-2");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            // Local cluster can not be filtered out for now
            assertClusterMetadataSkipped(localCluster, localShards, overallTookMillis, "logs-1");
        }

        // Both indices are filtered out - wildcards
        filter = new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now");
        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-*", randomBoolean(), filter)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(0));
            // assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), hasItems("@timestamp", "tag-local", "tag-cluster-a"));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // Remote has no shards due to filter
            assertClusterMetadataNoShards(remoteCluster, remoteShards, overallTookMillis, "logs-*");

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            // Local cluster can not be filtered out for now
            assertClusterMetadataSkipped(localCluster, localShards, overallTookMillis, "logs-*");
        }

    }

    public void testFilterWithMissingIndex() {
        int docsTest1 = 50;
        int docsTest2 = 30;
        int localShards = randomIntBetween(1, 5);
        int remoteShards = randomIntBetween(1, 5);
        populateDateIndex(LOCAL_CLUSTER, LOCAL_INDEX, localShards, docsTest1, "2024-11-26");
        populateDateIndex(REMOTE_CLUSTER_1, REMOTE_INDEX, remoteShards, docsTest2, "2023-11-26");

        int docSize = docsTest1;
        for (var filter : List.of(
            new RangeQueryBuilder("@timestamp").from("2024-01-01").to("now"),
            new RangeQueryBuilder("@timestamp").from("2025-01-01").to("now")
        )) {
            // Local index missing
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> runQuery("from missing", randomBoolean(), filter).close()
            );
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Local index missing + wildcards
            // FIXME: planner does not catch this now
            // e = expectThrows(VerificationException.class, () -> runQuery("from missing,logs*", randomBoolean(), filter).close());
            // assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Local index missing + existing index
            // FIXME: planner does not catch this now
            // e = expectThrows(VerificationException.class, () -> runQuery("from missing,logs-1", randomBoolean(), filter).close());
            // assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Local index missing + existing remote
            e = expectThrows(VerificationException.class, () -> runQuery("from missing,c*:logs-2", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing]"));
            // Wildcard index missing
            e = expectThrows(VerificationException.class, () -> runQuery("from missing*", randomBoolean(), filter).close());
            assertThat(e.getDetailedMessage(), containsString("Unknown index [missing*]"));
            // Wildcard index missing + existing index
            try (EsqlQueryResponse resp = runQuery("from missing*,logs-1", randomBoolean(), filter)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(docSize));
                // for the second round
                docSize = 0;
            }
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
