/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * A dedicated test suite for testing time series esql functionality.
 * This while the functionality is gated behind a query pragma.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DownsampledRateRestEsqlIT extends ESRestTestCase {
    public static final String INDEX_NAME = "counter-data";
    public static final String DOWNSAMPLED_INDEX_NAME = "counter-data-downsampled";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTimeSeriesQuerying() throws IOException {
        createIndex();
        indexDocuments(INDEX_NAME, List.of(
            Tuple.tuple("2021-04-29T17:02:12.470Z", 1),
            Tuple.tuple("2021-04-29T17:03:12.470Z", 2),
            Tuple.tuple("2021-04-29T17:10:12.470Z", 5),
            Tuple.tuple("2021-04-29T17:22:22.470Z", 6),
            Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
            Tuple.tuple("2021-04-29T17:29:22.470Z", 11),
            Tuple.tuple("2021-04-29T17:32:22.470Z", 12),
            Tuple.tuple("2021-04-29T17:39:22.470Z", 13)
        ));

        List<List<?>> values = queryRate(INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.00373967862485592, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));

        performDownsampling(INDEX_NAME, DOWNSAMPLED_INDEX_NAME);

        values = queryRate(DOWNSAMPLED_INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.00373967862485592, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));
    }

    public void testTimeSeriesQueryingSingleReset() throws IOException {
        createIndex();

        indexDocuments(INDEX_NAME, List.of(
            Tuple.tuple("2021-04-29T17:02:12.470Z", 1),
            Tuple.tuple("2021-04-29T17:03:12.470Z", 2),
            Tuple.tuple("2021-04-29T17:10:12.470Z", 5),
            Tuple.tuple("2021-04-29T17:19:12.470Z", 8),
            Tuple.tuple("2021-04-29T17:20:22.470Z", 3),
            Tuple.tuple("2021-04-29T17:22:22.470Z", 6),
            Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
            Tuple.tuple("2021-04-29T17:29:22.470Z", 11),
            Tuple.tuple("2021-04-29T17:32:22.470Z", 12),
            Tuple.tuple("2021-04-29T17:39:22.470Z", 13)
        ));

        List<List<?>> values = queryRate(INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.006111111111111111, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));

        performDownsampling(INDEX_NAME, DOWNSAMPLED_INDEX_NAME);

        values = queryRate(DOWNSAMPLED_INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.006111111111111111, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));
    }

    public void testTimeSeriesQueryingSingleLargeReset() throws IOException {
        createIndex();

        indexDocuments(INDEX_NAME, List.of(
            Tuple.tuple("2021-04-29T17:02:12.470Z", 1000),
            Tuple.tuple("2021-04-29T17:03:12.470Z", 1003),
            Tuple.tuple("2021-04-29T17:10:12.470Z", 1010),
            Tuple.tuple("2021-04-29T17:19:12.470Z", 1040),
            Tuple.tuple("2021-04-29T17:20:22.470Z", 1060),
            Tuple.tuple("2021-04-29T17:22:22.470Z", 20),
            Tuple.tuple("2021-04-29T17:24:22.470Z", 30),
            Tuple.tuple("2021-04-29T17:29:22.470Z", 40),
            Tuple.tuple("2021-04-29T17:32:22.470Z", 70),
            Tuple.tuple("2021-04-29T17:39:22.470Z", 80)
        ));

        List<List<?>> values = queryRate(INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.04314347284554129, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));

        performDownsampling(INDEX_NAME, DOWNSAMPLED_INDEX_NAME);

        values = queryRate(DOWNSAMPLED_INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.04314347284554129, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));
    }

    public void testTimeSeriesQueryingMultipleResets() throws IOException {
        createIndex();

        indexDocuments(INDEX_NAME, List.of(
            Tuple.tuple("2021-04-29T17:02:12.470Z", 1000),
            Tuple.tuple("2021-04-29T17:03:12.470Z", 1003),
            Tuple.tuple("2021-04-29T17:05:12.470Z", 1010),
            Tuple.tuple("2021-04-29T17:06:12.470Z", 1040),
            Tuple.tuple("2021-04-29T17:07:22.470Z", 1060),
            Tuple.tuple("2021-04-29T17:08:22.470Z", 20),
            Tuple.tuple("2021-04-29T17:10:22.470Z", 30),
            Tuple.tuple("2021-04-29T17:11:22.470Z", 40),
            Tuple.tuple("2021-04-29T17:12:22.470Z", 70),
            Tuple.tuple("2021-04-29T17:22:22.470Z", 80),
            Tuple.tuple("2021-04-29T17:23:22.470Z", 20),
            Tuple.tuple("2021-04-29T17:24:22.470Z", 10),
            Tuple.tuple("2021-04-29T17:25:22.470Z", 20),
            Tuple.tuple("2021-04-29T17:26:22.470Z", 40),
            Tuple.tuple("2021-04-29T17:27:22.470Z", 60),
            Tuple.tuple("2021-04-29T17:28:22.470Z", 5),
            Tuple.tuple("2021-04-29T17:29:22.470Z", 10),
            Tuple.tuple("2021-04-29T17:59:22.470Z", 20)
        ));

        List<List<?>> values = queryRate(INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.06997084548104959, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));

        performDownsampling(INDEX_NAME, DOWNSAMPLED_INDEX_NAME);

        values = queryRate(DOWNSAMPLED_INDEX_NAME);
        assertEquals(1, values.size());
        assertEquals(0.06997084548104959, values.get(0).get(0));
        assertEquals("2021-04-29T17:00:00.000Z", values.get(0).get(1));
    }

    @SuppressWarnings("unchecked")
    private List<List<?>> queryRate(String indexName) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("TS " + indexName + " | STATS max_rate=MAX(RATE(counter)) BY time_bucket = TBUCKET(1 hour) | SORT time_bucket");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> columns = (List<Map<?, ?>>) result.get("columns");
        assertEquals(2, columns.size());
        assertEquals("max_rate", columns.get(0).get("name"));
        assertEquals("time_bucket", columns.get(1).get("name"));
        return (List<List<?>>) result.get("values");
    }

    private void performDownsampling(String index, String downsampledIndex) throws IOException {
        Request readOnly = new Request("PUT", "/" + index + "/_settings");
        readOnly.setJsonEntity("{\"index.blocks.read_only_allow_delete\": true}");
        assertAcknowledged(client().performRequest(readOnly));
        Request downsample = new Request("POST", "/" + index + "/_downsample/" + downsampledIndex);
        downsample.setJsonEntity("{\"fixed_interval\": \"30m\"}");
        assertAcknowledged(client().performRequest(downsample));
    }

    private void indexDocuments(String indexName, List<Tuple<String, Integer>> documents) throws IOException {
        Request bulk = new Request("POST", "/" + indexName + "/_bulk");
        bulk.addParameter("refresh", "true");
        StringBuilder builder = new StringBuilder();
        for (Tuple<String, Integer> document : documents) {
            builder.append("{\"create\": {}}\n");
            builder.append("{\"@timestamp\": \"");
            builder.append(document.v1());
            builder.append("\", \"metricset\": \"pod\", \"counter\":");
            builder.append(document.v2());
            builder.append(", \"k8s\": {\"pod\": {\"name\": \"cat\", \"uid\":\"947e4ced-1786-4e53-9e0c-5c447e959507\"}}}\n");
        }
        bulk.setJsonEntity(builder.toString());
        assertIndexedDocuments(entityAsMap(client().performRequest(bulk)));
    }

    @SuppressWarnings("unchecked")
    private void assertIndexedDocuments(Map<String, Object> response) {
        if (Objects.equals(response.get("errors"), true)) {
            List<Map<?, Map<?, ?>>> items = (List<Map<?, Map<?, ?>>>) response.get("items");
            for (Map<?, Map<?, ?>> item : items) {
                for (Map<?, ?> docResponse : item.values()) {
                    Map<?, ?> error = (Map<?, ?>) docResponse.get("error");
                    if (error != null) {
                        fail("Failed to index documents: " + error.get("reason"));
                    }
                }
            }
        }
    }

    private static void createIndex() throws IOException {
        var settings = Settings.builder()
            .loadFromStream("tsdb-settings.json", DownsampledRateRestEsqlIT.class.getResourceAsStream("/tsdb-settings.json"), false);
        if (IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG && randomBoolean()) {
            settings.put(IndexSettings.USE_SYNTHETIC_ID.getKey(), true);
        }
        String mapping = CsvTestsDataLoader.readTextFile(DownsampledRateRestEsqlIT.class.getResource("/tsdb-counter-mapping.json"));
        assertTrue("Failed to create index [" + INDEX_NAME + "]", createIndex(INDEX_NAME, settings.build(), mapping).isAcknowledged());
    }
}
