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
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    private static void checkSubqueryWithRowSupport() {
        assumeTrue("Requires subquery with ROW as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    private static void checkSubqueryWithTSSupport() {
        assumeTrue("Requires subquery with TS as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
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
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
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
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM missing*,
                (FROM c*:missing* metadata _index),
                (FROM r*:missing* metadata _index)
            """, randomBoolean())) {
            assertThat(getValuesList(resp), hasSize(0));
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM c*:missing* metadata _index),
                (FROM r*:missing* metadata _index)
            """, randomBoolean())) {
            assertThat(getValuesList(resp), hasSize(0));
        }

        // If there is no subquery with a valid index pattern, the query should fail.
        expectThrows(VerificationException.class, containsString("Unknown index [cluster-a:missing,cluster-a:remote]"), () -> runQuery("""
            FROM (FROM c*:remote metadata _index),(FROM c*:missing metadata _index) metadata _index
            | STATS c = count(*) by _index
            | SORT _index
            """, randomBoolean()));

        expectThrows(
            VerificationException.class,
            allOf(containsString("Unknown index [missing]"), containsString("Unknown index [cluster-a:missing,cluster-a:remote]")),
            () -> runQuery("""
                FROM missing, (FROM c*:remote metadata _index),(FROM c*:missing metadata _index) metadata _index
                | STATS c = count(*) by _index
                | SORT _index
                """, randomBoolean())
        );
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

    public void testSubqueryWithLookupJoinInSubquery() {
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
    }

    public void testLocalJoinAndRemoteSubquery() {
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);

        try (var response = runQuery("""
            FROM
                (FROM logs-* | STATS mv = max(v) BY tag | LOOKUP JOIN values_lookup on mv == lookup_key),
                (FROM *:logs-* | STATS mv = max(v) BY tag)
            | KEEP tag, lookup_tag, mv
            | SORT tag
            """, randomBoolean())) {
            assertEquals(
                getValuesList(response),
                List.of(
                    List.of("local", "local", 9L),
                    // lookup_tag is only populated on local indices above and not joined on remote indices below
                    Arrays.asList("remote", null, 81L)
                )
            );
            assertCCSExecutionInfoDetails(response.getExecutionInfo());
        }
    }

    public void testSubqueryWithLookupJoinInMainQuery() {
        assumeTrue(
            "Requires subquery in FROM command with implicit LIMIT removed",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        // lookup join in main query after subqueries is not supported yet, as UnionAll is executed on the coordinating node
        // TODO lookup join cannot be executed after UnionAll, either rewrite the query or wait until this limitation is lifted
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
            |  LOOKUP JOIN values_lookup on v == lookup_key
            """, randomBoolean()));
        assertThat(
            ex.getMessage(),
            containsString("LOOKUP JOIN with remote indices can't be executed after [logs-*,(FROM c*:logs-*), (FROM r*:logs-*)]")
        );
    }

    public void testSubqueryWithLookupJoinIndicesExistOnAllClustersReferencedBySubqueries() {
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup_1", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup_2", 10);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        String query = """
            FROM
                logs-*,
                (FROM cluster-a:logs-* metadata _index
                 | where v > 1
                 | LOOKUP JOIN values_lookup_1 on v == lookup_key),
                (FROM remote-b:logs-* metadata _index
                 | where v > 1
                 | LOOKUP JOIN values_lookup_2 on v == lookup_key),
                (FROM logs-*, *:logs-* metadata _index
                 | where v > 1
                 | LOOKUP JOIN values_lookup on v == lookup_key)
                metadata _index
            | WHERE v < 5
            | EVAL lookup_tag = coalesce(lookup_tag, "local")
            | KEEP tag, v, _index, lookup_tag
            | SORT tag, v, _index
            """;

        try (EsqlQueryResponse resp = runQuery(query, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "_index", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("local", 0L, LOCAL_INDEX, "local"),
                List.of("local", 1L, LOCAL_INDEX, "local"),
                List.of("local", 2L, LOCAL_INDEX, "local"),
                List.of("local", 2L, LOCAL_INDEX, "local"),
                List.of("local", 3L, LOCAL_INDEX, "local"),
                List.of("local", 3L, LOCAL_INDEX, "local"),
                List.of("local", 4L, LOCAL_INDEX, "local"),
                List.of("local", 4L, LOCAL_INDEX, "local"),
                List.of("remote", 4L, REMOTE_CLUSTER_1_INDEX, REMOTE_CLUSTER_1),
                List.of("remote", 4L, REMOTE_CLUSTER_1_INDEX, REMOTE_CLUSTER_1),
                List.of("remote", 4L, REMOTE_CLUSTER_2_INDEX, REMOTE_CLUSTER_2),
                List.of("remote", 4L, REMOTE_CLUSTER_2_INDEX, REMOTE_CLUSTER_2)

            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    /**
     * A {@code LOOKUP JOIN} inside a subquery references a lookup index that does not exist on any cluster referenced by the subquery. A
     * completely missing lookup index is an analysis-time error - index resolution returns an invalid resolution ("Unknown index"), which
     * short-circuits before the skip-vs-error decision is reached. The behavior is therefore independent of {@code skip_unavailable}: the
     * query always fails with a {@link VerificationException}, identically for {@code skip_unavailable=true} and {@code false}, same
     * behavior as CrossClusterLookupJoinIT.testLookupJoinMissingRemoteIndex. This test runs every case under both settings to capture that
     * the behavior does not change.
     */
    public void testSubqueryWithLookupJoinMissingLookupIndexOnSomeClusters() {
        // values_lookup exists only on cluster-a; missing_lookup is never created, so it is absent from every cluster.
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup_1", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup_2", 10);

        // (1) lookup join in a subquery scoped to a single remote cluster: missing only on that cluster.
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                logs-*,
                (FROM cluster-a:logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // (2) lookup join in a subquery scoped to the local cluster: missing locally.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                cluster-a:logs-*,
                (FROM logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [missing_lookup]"));

        // (3) two remote subqueries joining the same missing lookup index: the lookup is scoped to both remotes, so the missing
        // index is reported for both clusters.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                logs-*,
                (FROM cluster-a:logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key),
                (FROM remote-b:logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup,remote-b:missing_lookup]"));

        // (4) one subquery uses an existing lookup index (values_lookup on remote-b), the sibling subquery references the missing
        // lookup index: the missing index fails the query, but it is reported only for remote-b because that is the only cluster
        // relevant to its LOOKUP JOIN - cluster-a is not queried for missing_lookup since it belongs to the sibling subquery.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (FROM remote-b:logs-* metadata _index | LOOKUP JOIN values_lookup_2 ON v == lookup_key),
                (FROM remote-b:logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(
            ex.getMessage(),
            allOf(containsString("Unknown index [remote-b:missing_lookup]"), not(containsString("cluster-a:missing_lookup")))
        );

        // (5) one subquery on local, plus a remote subquery whose lookup index is missing.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (FROM logs-*),
                (FROM cluster-a:logs-* metadata _index | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // (6) lookup index missing on remote-b, but exists on cluster-a
        // cluster-a has skipUnavailable=false by default in this test suite
        String query = """
            FROM
                (FROM logs-*),
                (FROM *:logs-* metadata _index | LOOKUP JOIN values_lookup_2 ON v == lookup_key)
            """;

        ex = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()));
        assertThat(ex.getMessage(), containsString("lookup index [values_lookup_2] is not available in remote cluster [cluster-a]"));
        // validate the behavior of skipUnavailable on cluster-a, lookup index exists on remote-b but not on cluster-a
        try {
            setSkipUnavailable(REMOTE_CLUSTER_1, true);
            try (EsqlQueryResponse resp = runQuery(query, randomBoolean())) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(20)); // 10 docs from local cluster, 10 from remote-b
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SKIPPED);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
            }
        } finally {
            setSkipUnavailable(REMOTE_CLUSTER_1, false);
        }
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
    }

    public void testSubqueryWithInlineStatsInMainQuery() {
        assumeTrue(
            "Requires subquery in FROM command with implicit LIMIT removed",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
        // inline stats in main query after subqueries is supported
        try (EsqlQueryResponse resp = runQuery("""
            FROM logs-*,(FROM c*:logs-*), (FROM r*:logs-*)
            | INLINE STATS sum = sum(v) BY tag
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
                if (i < 10) {
                    assertEquals(45L, row.get(sumIndex));
                    assertEquals("local", row.get(tagIndex));
                    assertEquals((long) i, row.get(vIndex));
                } else {
                    assertEquals(570L, row.get(sumIndex));
                    assertEquals("remote", row.get(tagIndex));
                }
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
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

    public void testSubqueryWithRow() {
        checkSubqueryWithRowSupport();

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | STATS c = count(*) | EVAL cluster = "local"),
                (FROM *:logs-* | STATS c = count(*) | EVAL cluster = "remote"),
                (ROW c = TO_LONG(99), cluster = "row")
            | KEEP c, cluster
            | SORT cluster
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("c", "cluster"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(List.of(10L, "local"), List.of(20L, "remote"), List.of(99L, "row"));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | WHERE v < 2 | KEEP tag, v),
                (FROM *:logs-* | WHERE v < 1 | KEEP tag, v),
                (ROW tag = "row", v = TO_LONG(100))
            | KEEP tag, v
            | SORT tag, v
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("local", 0L),
                List.of("local", 1L),
                List.of("remote", 0L),
                List.of("remote", 0L),
                List.of("row", 100L)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithRowAndLookupJoin() {
        checkSubqueryWithRowSupport();
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | where v == 6 | LOOKUP JOIN values_lookup on v == lookup_key),
                (FROM *:logs-* | where v == 4 | LOOKUP JOIN values_lookup on v == lookup_key),
                (ROW v = TO_LONG(4), tag = "row" | LOOKUP JOIN values_lookup on v == lookup_key)
            | KEEP tag, v, lookup_tag
            | SORT tag, v, lookup_tag
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("local", 6L, "local"),
                List.of("remote", 4L, REMOTE_CLUSTER_1),
                List.of("remote", 4L, REMOTE_CLUSTER_2),
                List.of("row", 4L, "local")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    // Same limitation as testSubqueryWithLookupJoinInMainQuery
    public void testSubqueryWithRowAndLookupJoinInMainQuery() {
        checkSubqueryWithRowSupport();
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 1);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 1);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 1);

        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (FROM logs-* | where v == 6),
                (FROM *:logs-* | where v == 4),
                (ROW v = TO_LONG(4), tag = "row")
            |  LOOKUP JOIN values_lookup on v == lookup_key
            """, randomBoolean()));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("LOOKUP JOIN with remote indices can't be executed after [(FROM logs-* | where v == 6),"),
                containsString("(FROM *:logs-* | where v == 4),"),
                containsString("(ROW v = TO_LONG(4), tag = \"row\")]")
            )
        );
    }

    public void testSubqueryWithRowAndLookupIndicesExistOnClustersReferencedBySubquery() {
        checkSubqueryWithRowSupport();
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup_remote", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup_remote", 10);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup_local", 10);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | where v == 6 | LOOKUP JOIN values_lookup_local on v == lookup_key),
                (FROM *:logs-* | where v == 4 | LOOKUP JOIN values_lookup_remote on v == lookup_key),
                (ROW v = TO_LONG(4), tag = "row" | LOOKUP JOIN values_lookup_local on v == lookup_key)
            | KEEP tag, v, lookup_tag
            | SORT tag, v, lookup_tag
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("tag", "v", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("local", 6L, "local"),
                List.of("remote", 4L, REMOTE_CLUSTER_1),
                List.of("remote", 4L, REMOTE_CLUSTER_2),
                List.of("row", 4L, "local")
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithRowAndLookupIndicesMissingOnClustersReferencedBySubquery() {
        checkSubqueryWithRowSupport();

        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                cluster-a:logs-*,
                (ROW v = TO_LONG(4))
            | LOOKUP JOIN missing_lookup ON v == lookup_key
            """, randomBoolean()));
        // The main-query LOOKUP JOIN reads from both cluster-a (logs-*) and the local cluster (the ROW branch), so the
        // lookup index is resolved against both and the single combined field-caps request reports both as missing.
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup,missing_lookup]"));

        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                cluster-a:logs-*,
                (ROW v = TO_LONG(4) | LOOKUP JOIN missing_lookup ON v == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [missing_lookup]"));
    }

    public void testSubqueryWithTS() {
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(LOCAL_CLUSTER, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (TS metrics | STATS m = max(cpu) | EVAL cluster = "local"),
                (TS cluster-a:metrics | STATS m = max(cpu) | EVAL cluster = "cluster-a"),
                (TS remote-b:metrics | STATS m = max(cpu) | EVAL cluster = "remote-b")
            | KEEP cluster, m
            | SORT cluster
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("cluster", "m"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(List.of(REMOTE_CLUSTER_1, 6.0), List.of("local", 6.0), List.of(REMOTE_CLUSTER_2, 6.0));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (TS metrics | WHERE host == "h1" | STATS m = max(cpu) | EVAL cluster = "local"),
                (TS cluster-a:metrics | WHERE host == "h1" | STATS m = max(cpu) | EVAL cluster = "cluster-a"),
                (TS remote-b:metrics | WHERE host == "h1" | STATS m = max(cpu) | EVAL cluster = "remote-b")
            | KEEP cluster, m
            | SORT cluster
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("cluster", "m"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(List.of(REMOTE_CLUSTER_1, 3.0), List.of("local", 3.0), List.of(REMOTE_CLUSTER_2, 3.0));
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithTSAndLookupJoin() {
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (TS cluster-a:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag),
                (TS remote-b:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag)
            | SORT cluster_tag, key
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("cluster_tag", "key", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of(REMOTE_CLUSTER_1, 1L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 2L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 3L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_2, 1L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 2L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 3L, REMOTE_CLUSTER_2)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    // Same limitation as testSubqueryWithLookupJoinInMainQuery
    public void testSubqueryWithTSAndLookupJoinInMainQuery() {
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 1);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 1);

        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (TS cluster-a:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | KEEP cluster_tag, key),
                (TS remote-b:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | KEEP cluster_tag, key)
            | LOOKUP JOIN values_lookup ON key == lookup_key
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("LOOKUP JOIN with remote indices can't be executed after [(TS cluster-a:metrics"));
    }

    public void testSubqueryWithTSAndLookupIndicesExistOnClustersReferencedBySubquery() {
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup_1", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup_2", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (TS cluster-a:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup_1 ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag),
                (TS remote-b:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup_2 ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag)
            | SORT cluster_tag, key
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("cluster_tag", "key", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of(REMOTE_CLUSTER_1, 1L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 2L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 3L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_2, 1L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 2L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 3L, REMOTE_CLUSTER_2)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (TS cluster-a:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup_1 ON key == lookup_key
                 | RENAME lookup_key AS lookup_key_1, lookup_tag AS lookup_tag_1
                 | LOOKUP JOIN values_lookup ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag),
                (TS remote-b:metrics
                 | WHERE host == "h1"
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup_2 ON key == lookup_key
                 | RENAME lookup_key AS lookup_key_2, lookup_tag AS lookup_tag_2
                 | LOOKUP JOIN values_lookup ON key == lookup_key
                 | KEEP cluster_tag, key, lookup_tag)
            | SORT cluster_tag, key
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("cluster_tag", "key", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of(REMOTE_CLUSTER_1, 1L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 2L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_1, 3L, REMOTE_CLUSTER_1),
                List.of(REMOTE_CLUSTER_2, 1L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 2L, REMOTE_CLUSTER_2),
                List.of(REMOTE_CLUSTER_2, 3L, REMOTE_CLUSTER_2)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithTSAndLookupIndexMissingOnClustersReferencedBySubquery() {
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");

        // (1) TS subquery scoped to a single remote cluster: the missing lookup is reported only for that cluster.
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (ROW key = TO_LONG(4)),
                (TS cluster-a:metrics
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN missing_lookup ON key == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (TS cluster-a:metrics
                 | EVAL key = TO_LONG(cpu)),
                (TS remote-b:metrics
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN missing_lookup ON key == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [remote-b:missing_lookup]"));

        // (2) two TS subqueries joining the same missing lookup index: the lookup is scoped to both remotes.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM
                (TS cluster-a:metrics
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN missing_lookup ON key == lookup_key),
                (TS remote-b:metrics
                 | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN missing_lookup ON key == lookup_key)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup,remote-b:missing_lookup]"));
    }

    public void testSubqueryWithMixedSources() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(LOCAL_CLUSTER, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | STATS val = count(*) | EVAL src = "from-local"),
                (FROM *:logs-* | STATS val = count(*) | EVAL src = "from-remote"),
                (TS metrics | STATS val = TO_LONG(max(cpu)) | EVAL src = "ts-local"),
                (TS *:metrics | STATS val = TO_LONG(max(cpu)) | EVAL src = "ts-remote"),
                (ROW src = "row", val = TO_LONG(99))
            | KEEP src, val
            | SORT src
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("src", "val"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("from-local", 10L),
                List.of("from-remote", 20L),
                List.of("row", 99L),
                List.of("ts-local", 6L),
                List.of("ts-remote", 6L)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithMixedSourcesWithoutAgg() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | WHERE v < 2 | EVAL src = "from-local", key = v | KEEP src, key),
                (FROM *:logs-* | WHERE v < 2 | EVAL src = "from-remote", key = v | KEEP src, key),
                (TS cluster-a:metrics | WHERE host == "h1" | EVAL src = "ts-remote", key = TO_LONG(cpu) | KEEP src, key),
                (ROW src = "row", key = TO_LONG(100))
            | KEEP src, key
            | SORT src, key
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("src", "key"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("from-local", 0L),
                List.of("from-local", 1L),
                List.of("from-remote", 0L),
                List.of("from-remote", 0L),
                List.of("from-remote", 1L),
                List.of("from-remote", 1L),
                List.of("row", 100L),
                List.of("ts-remote", 1L),
                List.of("ts-remote", 2L),
                List.of("ts-remote", 3L)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testSubqueryWithMixedSourcesAndLookupJoin() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        try (EsqlQueryResponse resp = runQuery("""
            FROM
                (FROM logs-* | WHERE v == 6 | LOOKUP JOIN values_lookup ON v == lookup_key
                 | EVAL src = "from-local", key = v | KEEP src, key, lookup_tag),
                (FROM *:logs-* | WHERE v == 4 | LOOKUP JOIN values_lookup ON v == lookup_key
                 | EVAL src = "from-remote", key = v | KEEP src, key, lookup_tag),
                (TS cluster-a:metrics | WHERE host == "h1" | EVAL key = TO_LONG(cpu)
                 | LOOKUP JOIN values_lookup ON key == lookup_key
                 | EVAL src = "ts-remote" | KEEP src, key, lookup_tag),
                (ROW key = TO_LONG(4) | LOOKUP JOIN values_lookup ON key == lookup_key
                 | EVAL src = "row" | KEEP src, key, lookup_tag)
            | KEEP src, key, lookup_tag
            | SORT src, key, lookup_tag
            """, randomBoolean())) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("src", "key", "lookup_tag"));

            List<List<Object>> values = getValuesList(resp);
            List<List<Object>> expected = List.of(
                List.of("from-local", 6L, "local"),
                List.of("from-remote", 4L, REMOTE_CLUSTER_1),
                List.of("from-remote", 4L, REMOTE_CLUSTER_2),
                List.of("row", 4L, "local"),
                List.of("ts-remote", 1L, REMOTE_CLUSTER_1),
                List.of("ts-remote", 2L, REMOTE_CLUSTER_1),
                List.of("ts-remote", 3L, REMOTE_CLUSTER_1)
            );
            assertEquals(expected, values);

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    /**
     * Mix FROM, TS and ROW subquery branches where a branch aggregates (STATS) before performing a LOOKUP JOIN. A remote
     * lookup join cannot run after a pipeline breaker, so even though the surrounding query is a mix of sources, verification
     * rejects the lookup that follows the STATS in the cross-cluster branch.
     */
    public void testSubqueryWithMixedSourcesAndLookupJoinAfterStats() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, "metrics");
        populateTimeSeriesIndex(REMOTE_CLUSTER_2, "metrics");
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 20);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 20);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 20);

        expectThrows(
            VerificationException.class,
            containsString("LOOKUP JOIN with remote indices can't be executed after [STATS key = TO_LONG(max(cpu))]"),
            () -> runQuery("""
                FROM
                    (FROM logs-* | STATS key = count(*) | EVAL src = "from-local"
                     | LOOKUP JOIN values_lookup ON key == lookup_key | KEEP src, key, lookup_tag),
                    (TS *:metrics | WHERE host == "h1" | STATS key = TO_LONG(max(cpu)) | EVAL src = "ts-remote"
                     | LOOKUP JOIN values_lookup ON key == lookup_key | KEEP src, key, lookup_tag),
                    (ROW src = "row", key = TO_LONG(5) | LOOKUP JOIN values_lookup ON key == lookup_key
                     | KEEP src, key, lookup_tag)
                | KEEP src, key, lookup_tag
                | SORT src, key
                """, randomBoolean())
        );
    }

    private void populateTimeSeriesIndex(String clusterAlias, String indexName) {
        String clusterTag = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("host"))
            .put("index.number_of_shards", randomIntBetween(1, 3))
            .build();
        Client client = client(clusterAlias);
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cluster_tag",
                    "type=keyword",
                    "cpu",
                    "type=double,time_series_metric=gauge"
                )
        );
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        for (String host : List.of("h1", "h2")) {
            double base = host.equals("h1") ? 1.0 : 4.0;
            for (int i = 0; i < 3; i++) {
                client.prepareIndex(indexName)
                    .setSource("@timestamp", timestamp + i * 1000L, "host", host, "cluster_tag", clusterTag, "cpu", base + i)
                    .get();
            }
        }
        client.admin().indices().prepareRefresh(indexName).get();
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
