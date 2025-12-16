/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterSubqueryIT extends AbstractCrossClusterTestCase {

    private static final String REMOTE_CLUSTER_1_INDEX = REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX;
    private static final String REMOTE_CLUSTER_2_INDEX = REMOTE_CLUSTER_2 + ":" + REMOTE_INDEX;

    @Before
    public void checkSubqueryInFromCommandSupport() throws IOException {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        setupClusters(3);
    }

    public void testSubquery() {
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
                    assertEquals(REMOTE_CLUSTER_1_INDEX, indexName);
                } else if (i < 20) {
                    assertEquals(LOCAL_INDEX, indexName);
                } else {
                    assertEquals(REMOTE_CLUSTER_2_INDEX, indexName);
                }
                assertThat((Long) row.get(vIndex), greaterThanOrEqualTo(0L));
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithAliases() {
        String ALIAS = "logs-a";
        setupAlias(LOCAL_CLUSTER, LOCAL_INDEX, ALIAS);
        setupAlias(REMOTE_CLUSTER_1, REMOTE_INDEX, ALIAS);
        setupAlias(REMOTE_CLUSTER_2, REMOTE_INDEX, ALIAS);

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
                List.of(10L, REMOTE_CLUSTER_1_INDEX),
                List.of(10L, LOCAL_INDEX),
                List.of(10L, REMOTE_CLUSTER_2_INDEX)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithDateMath() {
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

    /*
     * Validate Analyzer.PruneEmptyUnionAllBranch when remote index is missing
     */
    public void testSubqueryWithMissingRemoteIndex() {
        populateIndex(LOCAL_CLUSTER, "local_idx", randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, "remote_idx", randomIntBetween(1, 5), 5);

        // all subqueries have at least one index matching the index pattern, query succeeds
        try (EsqlQueryResponse resp = runQuery("""
            FROM local*,(FROM *:remote* metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx"));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        // One subquery on remote cluster 1 does not have any index matching the index pattern,
        // remote cluster 1 is marked as skipped in executionInfo and Analyzer prunes that branch.
        try (EsqlQueryResponse resp = runQuery("""
            FROM local*,
                (FROM c*:remote* metadata _index),
                (FROM r*:remote* metadata _index)
              metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx"));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
        }

        // Multiple subqueries on remote cluster 1 do not have any index matching the index pattern,
        // remote cluster 1 is marked as skipped in executionInfo and Analyzer prunes those branches.
        try (EsqlQueryResponse resp = runQuery("""
            FROM local*,
                (FROM c*:remote* metadata _index),
                (FROM c*:missing* metadata _index),
                (FROM r*:remote* metadata _index)
              metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(List.of(5L, "local_idx"), List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx"));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
        }

        // Some subqueries on remote cluster 1 have indexes matching the index pattern, some don't
        // remote cluster 1 is marked as successful in executionInfo and Analyzer keeps prunes empty branches.
        try (EsqlQueryResponse resp = runQuery("""
            FROM local*,
                (FROM c*:remote* metadata _index),
                (FROM r*:remote* metadata _index),
                (FROM c*:logs-* metadata _index)
              metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            List<List<Object>> expected = List.of(
                List.of(10L, REMOTE_CLUSTER_1_INDEX),
                List.of(5L, "local_idx"),
                List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM local*,
                (FROM c*:remote* metadata _index),
                (FROM r*:remote*, c*:logs-* metadata _index)
              metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            List<List<Object>> expected = List.of(
                List.of(10L, REMOTE_CLUSTER_1_INDEX),
                List.of(5L, "local_idx"),
                List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        // If there is no subquery with matching index pattern, Analyzer's verifier fails on the query.
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [cluster-a:missing,cluster-a:remote]"),
            () -> runQuery("""
            FROM (FROM c*:remote metadata _index),(FROM c*:missing metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean())
        );

        expectThrows(VerificationException.class,
            allOf(
                containsString("Unknown index [missing]"),
                containsString("Unknown index [cluster-a:missing,cluster-a:remote]")
            ),
            () -> runQuery("""
            FROM missing, (FROM c*:remote metadata _index),(FROM c*:missing metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean()));
    }

    /*
     * Validate Analyzer.PruneEmptyUnionAllBranch when local index is missing
     */
    public void testSubqueryWithMissingLocalIndex() {
        populateIndex(REMOTE_CLUSTER_1, "remote_idx", randomIntBetween(1, 5), 5);
        populateIndex(REMOTE_CLUSTER_2, "remote_idx", randomIntBetween(1, 5), 5);

        try (EsqlQueryResponse resp = runQuery("""
            FROM missing*, (FROM *:remote* metadata _index) metadata _index
             | STATS c = count(*) by _index
             | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(
                List.of(5L, REMOTE_CLUSTER_1 + ":remote_idx"),
                List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM (FROM local* metadata _index),(FROM *:remote* metadata _index) metadata _index
             | STATS c = count(*) by _index
             | SORT _index
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "_index"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            List<List<Object>> expected = List.of(
                List.of(5L, REMOTE_CLUSTER_1 + ":remote_idx"),
                List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
        }

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
                List.of(10L, REMOTE_CLUSTER_1_INDEX),
                List.of(5L, REMOTE_CLUSTER_1 + ":remote_idx"),
                List.of(10L, LOCAL_INDEX),
                List.of(10L, REMOTE_CLUSTER_2_INDEX),
                List.of(5L, REMOTE_CLUSTER_2 + ":remote_idx")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithFilter() {
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
                List.of("remote", 4L, REMOTE_CLUSTER_1_INDEX),
                List.of("local", 6L, LOCAL_INDEX),
                List.of("remote", 4L, REMOTE_CLUSTER_2_INDEX)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithFullTextFunctionInFilter() {
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
                List.of("remote", 0L, REMOTE_CLUSTER_1_INDEX),
                List.of("remote", 1L, REMOTE_CLUSTER_1_INDEX),
                List.of("remote", 0L, REMOTE_CLUSTER_2_INDEX),
                List.of("remote", 1L, REMOTE_CLUSTER_2_INDEX)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                logs-*,
                (FROM *:logs-* metadata _index | WHERE tag:"remote")
                metadata _index
            | WHERE v < 2
            | DROP const, id
            | SORT _index, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("remote", 0L, REMOTE_CLUSTER_1_INDEX),
                List.of("remote", 1L, REMOTE_CLUSTER_1_INDEX),
                List.of("local", 0L, LOCAL_INDEX),
                List.of("local", 1L, LOCAL_INDEX),
                List.of("remote", 0L, REMOTE_CLUSTER_2_INDEX),
                List.of("remote", 1L, REMOTE_CLUSTER_2_INDEX)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithStats() {
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
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithLookupJoin() {
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
                List.of("remote", 4L, REMOTE_CLUSTER_1_INDEX, REMOTE_CLUSTER_1),
                List.of("local", 6L, LOCAL_INDEX, "local"),
                List.of("remote", 4L, REMOTE_CLUSTER_2_INDEX, REMOTE_CLUSTER_2)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        // lookup join in main query after subqueries is not supported yet, because there is remote index pattern and limit,
        // refer to LookupJoin.checkRemoteJoin
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

    public void testSubqueryWithInlineStatsInSubquery() {
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

    public void testSubqueryWithFilterInRequest() {
        String query = """
            FROM
                (FROM logs-* metadata _index | where v > 5),
                (FROM *:logs-* metadata _index | where v >1)
            | DROP const, id
            | SORT _index, v
            """;
        EsqlQueryRequest request = randomBoolean()
            ? EsqlQueryRequest.asyncEsqlQueryRequest(query)
            : EsqlQueryRequest.syncEsqlQueryRequest(query);
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
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testNestedSubqueries() {
        // nested subqueries are not supported yet
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*, (FROM r*:logs-*))
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Nested subqueries are not supported"));
    }

    public void testSubqueryWithFork() {
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

    static void assertClusterEsqlExecutionInfo(
        EsqlExecutionInfo executionInfo,
        String clusterAlias,
        EsqlExecutionInfo.Cluster.Status expectedStatus
    ) {
        var cluster = executionInfo.getCluster(clusterAlias);
        assertEquals(expectedStatus, cluster.getStatus());
    }

    static void assertClusterEsqlExecutionInfoFailureReason(EsqlExecutionInfo executionInfo, String clusterAlias, String expectedMessage) {
        var cluster = executionInfo.getCluster(clusterAlias);
        var failures = cluster.getFailures();
        assertThat(failures, not(empty()));
        assertThat(failures.get(0).reason(), containsString(expectedMessage));
    }
}
