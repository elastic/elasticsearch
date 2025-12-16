/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

// @TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterTimeSeriesIT extends AbstractCrossClusterTestCase {

    private static final String INDEX_NAME = "hosts";

    record Doc(String host, String cluster, long timestamp, int requestCount, double cpu, ByteSizeValue memory) {}

    public void testTsIdMetadataInResponse() {
        populateTimeSeriesIndex(LOCAL_CLUSTER, INDEX_NAME);
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, INDEX_NAME);

        try (EsqlQueryResponse resp = runQuery("TS hosts, cluster-a:hosts METADATA _tsid", Boolean.TRUE)) {
            assertNotNull(
                resp.columns().stream().map(ColumnInfoImpl::name).filter(name -> name.equalsIgnoreCase("_tsid")).findFirst().orElse(null)
            );

            assertCCSExecutionInfoDetails(resp.getExecutionInfo(), 2);
        }
    }

    public void testTsIdMetadataInResponseWithFailure() {
        populateTimeSeriesIndex(LOCAL_CLUSTER, INDEX_NAME);
        populateTimeSeriesIndex(REMOTE_CLUSTER_1, INDEX_NAME);

        try (
            EsqlQueryResponse resp = runQuery(
                "TS hosts, cluster-a:hosts METADATA _tsid | WHERE host IS NOT NULL | STATS cnt = count_distinct(_tsid)",
                Boolean.TRUE
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertNotNull(values.getFirst().getFirst());
            assertCCSExecutionInfoDetails(resp.getExecutionInfo(), 2);
        }
    }

    private void populateTimeSeriesIndex(String clusterAlias, String indexName) {
        int numShards = randomIntBetween(1, 5);
        String clusterTag = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("host", "cluster"))
            .put("index.number_of_shards", numShards)
            .build();

        client(clusterAlias).admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cluster",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=double,time_series_metric=gauge",
                "memory",
                "type=long,time_series_metric=gauge",
                "request_count",
                "type=integer,time_series_metric=counter",
                "cluster_tag",
                "type=keyword"
            )
            .get();

        final List<Doc> docs = getRandomDocs();

        for (Doc doc : docs) {
            client().prepareIndex(indexName)
                .setSource(
                    "@timestamp",
                    doc.timestamp,
                    "host",
                    doc.host,
                    "cluster",
                    doc.cluster,
                    "cpu",
                    doc.cpu,
                    "memory",
                    doc.memory.getBytes(),
                    "request_count",
                    doc.requestCount,
                    "cluster_tag",
                    clusterTag
                )
                .get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
    }

    private List<Doc> getRandomDocs() {
        final List<Doc> docs = new ArrayList<>();

        Map<String, String> hostToClusters = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            hostToClusters.put("p" + i, randomFrom("qa", "prod"));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");

        Map<String, Integer> requestCounts = new HashMap<>();
        int numDocs = between(20, 100);
        for (int i = 0; i < numDocs; i++) {
            List<String> hosts = randomSubsetOf(between(1, hostToClusters.size()), hostToClusters.keySet());
            timestamp += between(1, 10) * 1000L;
            for (String host : hosts) {
                var requestCount = requestCounts.compute(host, (k, curr) -> {
                    if (curr == null || randomInt(100) <= 20) {
                        return randomIntBetween(0, 10);
                    } else {
                        return curr + randomIntBetween(1, 10);
                    }
                });
                int cpu = randomIntBetween(0, 100);
                ByteSizeValue memory = ByteSizeValue.ofBytes(randomIntBetween(1024, 1024 * 1024));
                docs.add(new Doc(host, hostToClusters.get(host), timestamp, requestCount, cpu, memory));
            }
        }

        Randomness.shuffle(docs);

        return docs;
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo, int expectedNumClusters) {
        assertNotNull(executionInfo);
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());
        assertThat(executionInfo.getClusters().size(), equalTo(expectedNumClusters));

        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases().stream().map(executionInfo::getCluster).toList();

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        }
    }

}
