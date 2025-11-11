/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

// @TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterSubqueryIT extends AbstractCrossClusterTestCase {

    @Before
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    public void testSubquery() throws IOException {
        setupClusters(3);

        try (EsqlQueryResponse resp = runQuery("""
            FROM (FROM logs-* metadata _index),(FROM *:logs-* metadata _index)
            | SORT _index, id
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("id", "tag", "v", "const", "_index"));
            int constIndex = columns.indexOf("const");
            int vIndex = columns.indexOf("v");
            int indexIndex = columns.indexOf("_index");

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(30));
            for (int i = 0; i < values.size(); i++) {
                var row = values.get(i);
                assertThat(row, hasSize(5));
                assertNull(row.get(constIndex));
                String indexName = (String) row.get(indexIndex);
                if (i < 10) {
                    assertEquals("cluster-a:logs-2", indexName);
                } else if (i < 20) {
                    assertEquals("logs-1", indexName);
                } else {
                    assertEquals("remote-b:logs-2", indexName);
                }
                assertThat((Long) row.get(vIndex), greaterThanOrEqualTo(0L));
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithAliases() throws IOException {
        setupClusters(3);
        setupAlias(LOCAL_CLUSTER, "logs-1", "logs-a");
        setupAlias(REMOTE_CLUSTER_1, "logs-2", "logs-a");
        setupAlias(REMOTE_CLUSTER_2, "logs-2", "logs-a");

        try (EsqlQueryResponse resp = runQuery("""
            FROM logs-a,(FROM *:logs-a metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            List<List<Object>> expected = List.of(
                List.of(10L, "cluster-a:logs-2"),
                List.of(10L, "logs-1"),
                List.of(10L, "remote-b:logs-2")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithDateMath() throws IOException {
        setupClusters(3);

        ZonedDateTime nowUtc = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime nextMidnight = nowUtc.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        // If we're too close to midnight, we could create index with one day and query with another, and it'd fail.
        assumeTrue("Skip if too close to midnight", Duration.between(nowUtc, nextMidnight).toMinutes() >= 5);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);
        String indexName = "idx_" + nowUtc.format(formatter);

        populateIndex(LOCAL_CLUSTER, indexName, randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_1, indexName, randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, indexName, randomIntBetween(1, 5), 5);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
              (FROM <idx_{now/d}> | STATS c = count(*) | EVAL cluster = "local"),
              (FROM *:<idx_{now/d}> | STATS c = count(*) | EVAL cluster = "remote")
            | SORT c DESC
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "cluster"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(List.of(10L, "remote"), List.of(5L, "local"));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithMissingRemoteIndexSkipUnavailableTrue() throws IOException {
        setupClusters(3);
        populateIndex(LOCAL_CLUSTER, "local_idx", randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, "remote_idx", randomIntBetween(1, 5), 5);

        try {
            setSkipUnavailable(REMOTE_CLUSTER_1, true);
            try (EsqlQueryResponse resp = runQuery("""
                FROM local*,(FROM *:remote* metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("c", "_index"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(2));
                List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, "remote-b:remote_idx"));
                assertEquals(expected, values);

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getFailures(), empty());
            }

            // The subquery does not have any index matching the index pattern, Analyzer prunes that branch.
            try (EsqlQueryResponse resp = runQuery("""
                FROM local*,(FROM c*:remote* metadata _index),(FROM r*:remote* metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("c", "_index"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(2));
                List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, "remote-b:remote_idx"));
                assertEquals(expected, values);

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getFailures(), empty());
            }

            // If there is no valid subquery, Analyzer's verifier fails on the query.
            var ex = expectThrows(VerificationException.class, () -> runQuery("""
                FROM (FROM c*:remote* metadata _index),(FROM c*:missing* metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean()));
            String errorMessage = ex.getMessage();
            assertThat(errorMessage, containsString("Unknown index [c*:remote*]"));
            assertThat(errorMessage, containsString("Unknown index [c*:missing*]"));
        } finally {
            clearSkipUnavailable(3);
        }
    }

    public void testSubqueryWithMissingRemoteIndexSkipUnavailableFalse() throws IOException {
        setupClusters(3);
        populateIndex(LOCAL_CLUSTER, "local_idx", randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, "remote_idx", randomIntBetween(1, 5), 5);

        try {
            setSkipUnavailable(REMOTE_CLUSTER_1, false);
            try (EsqlQueryResponse resp = runQuery("""
                FROM local*,(FROM *:remote* metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean())) {
                var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
                assertThat(columns, hasItems("c", "_index"));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(2));
                List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, "remote-b:remote_idx"));
                assertEquals(expected, values);

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

                var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getFailures(), empty());
            }

            // The subquery does not have any index matching the index pattern, Analyzer's verifier fails on the branch.
            var ex = expectThrows(VerificationException.class, () -> runQuery("""
                FROM (FROM c*:remote* metadata _index),(FROM r*:remote* metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean()));
            assertThat(ex.getMessage(), containsString("Unknown index [c*:remote*]"));
        } finally {
            clearSkipUnavailable(3);
        }
    }

    public void testSubqueryWithMissingLocalIndex() throws IOException {
        setupClusters(3);
        populateIndex(REMOTE_CLUSTER_1, "remote_idx", randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, "remote_idx", randomIntBetween(1, 5), 5);

        // no local index exists, the query should fail regardless skipUnavailable=true or false,
        // index resolution in Analyzer will fail on the branch anyway, it does not prune the branch with missing indices
        var ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM  local*,(FROM *:remote* metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [local*]"));

        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM (FROM local* metadata _index),(FROM *:remote* metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [local*]"));

        try (EsqlQueryResponse resp = runQuery("""
            FROM *,(FROM *:* metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(5));
            List<List<Object>> expected = List.of(
                List.of(10L, "cluster-a:logs-2"),
                List.of(5L, "cluster-a:remote_idx"),
                List.of(10L, "logs-1"),
                List.of(10L, "remote-b:logs-2"),
                List.of(5L, "remote-b:remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // This is successful, given the index does not exist on remote-1 but exists on remote-2, is this as expected?
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getFailures(), empty());
        }
    }

    public void testSubqueryWithFilter() throws IOException {
        setupClusters(3);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* metadata _index | where v > 5),
                (FROM *:logs-* metadata _index | where v >1)
            | WHERE v < 7
            | DROP const, id
            | SORT _index, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("remote", 4L, "cluster-a:logs-2"),
                List.of("local", 6L, "logs-1"),
                List.of("remote", 4L, "remote-b:logs-2")
            );
            assertTrue(values.equals(expected));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithFullTextFunctionInFilter() throws IOException {
        setupClusters(3);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                logs-*,
                (FROM *:logs-* metadata _index | where v < 4 )
                metadata _index
            | WHERE tag:"remote"
            | DROP const, id
            | SORT _index, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("remote", 0L, "cluster-a:logs-2"),
                List.of("remote", 1L, "cluster-a:logs-2"),
                List.of("remote", 0L, "remote-b:logs-2"),
                List.of("remote", 1L, "remote-b:logs-2")
            );
            assertTrue(values.equals(expected));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithStats() throws IOException {
        setupClusters(3);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                logs-*,
                (FROM *:logs-* | where v < 4 )
            | WHERE v < 5
            | STATS sum = sum(v) BY tag
            | SORT tag
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "sum"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(List.of(10L, "local"), List.of(2L, "remote"));
            assertTrue(values.equals(expected));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithLookupJoin() throws IOException {
        setupClusters(3);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        // lookup join inside subquery is supported
        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* metadata _index | where v > 5 | LOOKUP JOIN values_lookup on v == lookup_key),
                (FROM *:logs-* metadata _index | where v >1 | LOOKUP JOIN values_lookup on v == lookup_key)
            | WHERE v < 7
            | KEEP tag, v, _index, lookup_tag
            | SORT _index, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("remote", 4L, "cluster-a:logs-2", "cluster-a"),
                List.of("local", 6L, "logs-1", "local"),
                List.of("remote", 4L, "remote-b:logs-2", "remote-b")
            );
            assertTrue(values.equals(expected));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        // lookup join in main query after subqueries is not supported yet, because there is remote index pattern and limit
        // TODO remove the limit added for subqueries
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
            |  LOOKUP JOIN values_lookup on v == lookup_key
            """, randomBoolean()));
        assertThat(
            ex.getMessage(),
            containsString("LOOKUP JOIN with remote indices can't be executed after [FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)]")
        );
    }

    public void testSubqueryWithInlineStatsInSubquery() throws IOException {
        setupClusters(3);

        // inline stats inside subquery is supported
        try (EsqlQueryResponse resp = runQuery("""
            FROM
                logs-*,
                (FROM *:logs-* | INLINE STATS sum = sum(v) WHERE v < 10 )
            | WHERE v < 4
            | DROP const, id
            | SORT tag, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "sum", "v"));
            int tagIndex = columns.indexOf("tag");
            int vIndex = columns.indexOf("v");
            int sumIndex = columns.indexOf("sum");
            List<List<Object>> values = getValuesList(resp);
            for (int i = 0; i < values.size(); i++) {
                var row = values.get(i);
                if (i < 4) {
                    assertNull(row.get(sumIndex));
                    assertEquals("local", row.get(tagIndex));
                    assertEquals((long) i, row.get(vIndex));
                } else {
                    assertEquals(28L, row.get(sumIndex));
                    assertEquals("remote", row.get(tagIndex));
                    assertEquals(i > 5 ? 1L : 0L, row.get(vIndex));
                }
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        // inline stats in main query after subqueries is not supported yet, because there is limit
        // TODO remove the limit added for subqueries
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
            |  INLINE STATS sum = sum(v)
            """, randomBoolean()));
        String errorMessage = ex.getMessage();
        assertThat(
            errorMessage,
            containsString(
                "INLINE STATS after subquery is not supported, "
                    + "as INLINE STATS cannot be used after an explicit or implicit LIMIT command"
            )
        );
        assertThat(
            errorMessage,
            containsString(
                "INLINE STATS cannot be used after an explicit or implicit LIMIT command, "
                    + "but was [INLINE STATS sum = sum(v)] after [FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)]"
            )
        );
    }

    public void testSubqueryWithFilterInRequest() throws IOException {
        setupClusters(3);

        EsqlQueryRequest request = randomBoolean() ? EsqlQueryRequest.asyncEsqlQueryRequest() : EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("""
            FROM
                (FROM logs-* metadata _index | where v > 5),
                (FROM *:logs-* metadata _index | where v >1)
            | DROP const, id
            | SORT _index, v
            """);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());
        request.filter(new RangeQueryBuilder("v").lt(7));
        request.waitForCompletionTimeout(timeValueSeconds(30));

        try (EsqlQueryResponse resp = runQuery(request)) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("remote", 4L, "cluster-a:logs-2"),
                List.of("local", 6L, "logs-1"),
                List.of("remote", 4L, "remote-b:logs-2")
            );
            assertTrue(values.equals(expected));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testNestedSubqueries() throws IOException {
        setupClusters(3);

        // nested subqueries are not supported yet
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*, (FROM r*:logs-*))
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Nested subqueries are not supported"));
    }

    public void testSubqueryWithFork() throws IOException {
        setupClusters(3);

        // fork after subqueries is not supported yet
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
            | FORK
              (WHERE v > 5)
              (WHERE v < 3)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("FORK after subquery is not supported"));

        // fork inside subquery is not supported yet
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                logs-*,
                (FROM c*:logs-*),
                (FROM r*:logs-*
                 | FORK (WHERE v > 5) (WHERE v < 3))
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("FORK inside subquery is not supported"));
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertNotNull(executionInfo);
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());
        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases().stream().map(executionInfo::getCluster).toList();

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        }
    }

    protected void setupAlias(String clusterAlias, String indexName, String aliasName) {
        Client client = client(clusterAlias);
        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = client.admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(aliasName));
        assertAcked(client.admin().indices().aliases(indicesAliasesRequestBuilder.request()));
    }
}
