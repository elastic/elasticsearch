/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 * Cross-cluster tests for {@code LET} named subqueries whose bodies read remote and date-math index patterns. The resolver passes
 * every pattern in a binding body that is not an earlier binding name through to index resolution untouched; these tests check that
 * such bodies then search the right clusters. csv-spec tests cannot name remote clusters or date-math indices, so they live here.
 *
 * <p>Data layout: each cluster hosts a {@code logs-*} index with 10 docs, {@code v} running 0 to 9; {@code tag} is "local" on
 * the local cluster and "remote" on both remotes.
 */
public class CrossClusterNamedSubqueryIT extends AbstractCrossClusterTestCase {

    private static final String REMOTE_CLUSTER_1_INDEX = REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX;
    private static final String REMOTE_CLUSTER_2_INDEX = REMOTE_CLUSTER_2 + ":" + REMOTE_INDEX;

    private Map<String, Object> clusterInfo;

    @Before
    public void setupClustersAndCheckCapability() throws IOException {
        assumeTrue("requires NAMED_SUBQUERY_LET capability", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        clusterInfo = setupClusters(3);
    }

    private int localShards() {
        return (int) clusterInfo.get("local.num_shards");
    }

    private int remote1Shards() {
        return (int) clusterInfo.get("remote1.num_shards");
    }

    private int remote2Shards() {
        return (int) clusterInfo.get("remote2.num_shards");
    }

    public void testBindingBodyWithRemotePattern() {
        try (EsqlQueryResponse resp = runQuery("""
            LET remote_logs = (FROM *:logs-* METADATA _index | STATS c = COUNT(*) BY _index);
            FROM remote_logs
            | SORT _index
            """, randomBoolean())) {
            assertEquals(List.of(List.of(10L, REMOTE_CLUSTER_1_INDEX), List.of(10L, REMOTE_CLUSTER_2_INDEX)), getValuesList(resp));
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(REMOTE_CLUSTER_1, remote1Shards(), REMOTE_CLUSTER_2, remote2Shards())
            );
        }
    }

    public void testBindingBodyMixingLocalAndRemotePatterns() {
        try (EsqlQueryResponse resp = runQuery("""
            LET all_logs = (FROM logs-*, *:logs-* METADATA _index | STATS c = COUNT(*) BY _index);
            FROM all_logs
            | SORT _index
            """, randomBoolean())) {
            assertEquals(
                List.of(List.of(10L, REMOTE_CLUSTER_1_INDEX), List.of(10L, LOCAL_INDEX), List.of(10L, REMOTE_CLUSTER_2_INDEX)),
                getValuesList(resp)
            );
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, localShards(), REMOTE_CLUSTER_1, remote1Shards(), REMOTE_CLUSTER_2, remote2Shards())
            );
        }
    }

    public void testBindingBodyExcludingRemoteCluster() {
        try (EsqlQueryResponse resp = runQuery(Strings.format("""
            LET some_remotes = (FROM *:logs-*, -%s:* METADATA _index | STATS c = COUNT(*) BY _index);
            FROM some_remotes
            """, REMOTE_CLUSTER_2), randomBoolean())) {
            assertEquals(List.of(List.of(10L, REMOTE_CLUSTER_1_INDEX)), getValuesList(resp));
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(REMOTE_CLUSTER_1, remote1Shards()));
        }
    }

    /**
     * A binding body that reads an earlier binding next to a remote pattern: the earlier binding is substituted, and the remote
     * pattern still reaches index resolution. {@code METADATA _class, _name} on the outer binding answers {@code subquery} and its
     * name for every row, whichever cluster the row came from.
     */
    public void testBindingBodyMixingEarlierBindingWithRemotePattern() {
        assumeTrue(
            "requires NAMED_SUBQUERY_LET_METADATA_CLASS_AND_NAME capability",
            EsqlCapabilities.Cap.NAMED_SUBQUERY_LET_METADATA_CLASS_AND_NAME.isEnabled()
        );
        try (EsqlQueryResponse resp = runQuery(Strings.format("""
            LET local_low = (FROM logs-* | WHERE v < 3);
            LET mixed = (FROM local_low, %s:logs-*);
            FROM mixed METADATA _class, _name
            | STATS c = COUNT(*) BY _class, _name, tag
            | SORT tag
            """, REMOTE_CLUSTER_1), randomBoolean())) {
            assertEquals(
                List.of(List.of(3L, "subquery", "mixed", "local"), List.of(10L, "subquery", "mixed", "remote")),
                getValuesList(resp)
            );
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, localShards(), REMOTE_CLUSTER_1, remote1Shards())
            );
        }
    }

    public void testBindingBodyWithDateMath() {
        ZonedDateTime nowUtc = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime nextMidnight = nowUtc.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        // If we're too close to midnight, we could create index with one day and query with another, and it'd fail.
        assumeTrue("Skip if too close to midnight", Duration.between(nowUtc, nextMidnight).toMinutes() >= 5);
        String indexName = "idx_" + nowUtc.format(DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));

        int localIdxShards = randomIntBetween(1, 5);
        int remote1IdxShards = randomIntBetween(1, 5);
        int remote2IdxShards = randomIntBetween(1, 5);
        populateIndex(LOCAL_CLUSTER, indexName, localIdxShards, 5);
        populateIndex(REMOTE_CLUSTER_1, indexName, remote1IdxShards, 5);
        populateIndex(REMOTE_CLUSTER_2, indexName, remote2IdxShards, 5);

        try (EsqlQueryResponse resp = runQuery("""
            LET today = (FROM <idx_{now/d}>, *:<idx_{now/d}> | STATS c = COUNT(*) BY tag);
            FROM today
            | SORT tag
            """, randomBoolean())) {
            assertEquals(List.of(List.of(5L, REMOTE_CLUSTER_1), List.of(5L, "local"), List.of(5L, REMOTE_CLUSTER_2)), getValuesList(resp));
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, localIdxShards, REMOTE_CLUSTER_1, remote1IdxShards, REMOTE_CLUSTER_2, remote2IdxShards)
            );
        }
    }
}
