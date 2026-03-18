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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * A dedicated test suite for testing time series esql functionality.
 * This while the functionality is gated behind a query pragma.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class TSDBRestEsqlIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTimeSeriesQuerying() throws IOException {
        var settings = Settings.builder()
            .loadFromStream("tsdb-settings.json", CsvTestsDataLoader.getResourceStream("/tsdb-settings.json"), false);
        if (IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG && randomBoolean()) {
            settings.put(IndexSettings.SYNTHETIC_ID.getKey(), true);
        }
        String mapping = CsvTestsDataLoader.getResourceString("/tsdb-k8s-mapping.json");
        assertTrue("Failed to create index [k8s]", createIndex("k8s", settings.build(), mapping).isAcknowledged());

        Request bulk = new Request("POST", "/k8s/_bulk");
        bulk.addParameter("refresh", "true");

        String bulkBody = new String(
            TSDBRestEsqlIT.class.getResourceAsStream("/tsdb-bulk-request.txt").readAllBytes(),
            StandardCharsets.UTF_8
        );
        bulk.setJsonEntity(bulkBody);
        assertIndexedDocuments(entityAsMap(client().performRequest(bulk)));

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM k8s | KEEP k8s.pod.name, @timestamp | SORT @timestamp, k8s.pod.name");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> columns = (List<Map<?, ?>>) result.get("columns");
        assertEquals(2, columns.size());
        assertEquals("k8s.pod.name", columns.get(0).get("name"));
        assertEquals("@timestamp", columns.get(1).get("name"));

        // Note that _tsid is a hashed value, so tsid no longer is sorted lexicographically.
        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals(8, values.size());
        assertEquals("2021-04-29T17:29:12.470Z", values.get(0).get(1));
        assertEquals("cat", values.get(0).get(0));

        assertEquals("2021-04-29T17:29:12.470Z", values.get(0).get(1));
        assertEquals("cow", values.get(1).get(0));

        assertEquals("2021-04-29T17:29:12.470Z", values.get(0).get(1));
        assertEquals("hamster", values.get(2).get(0));

        assertEquals("2021-04-29T17:29:12.470Z", values.get(0).get(1));
        assertEquals("rat", values.get(3).get(0));

        assertEquals("2021-04-29T17:29:22.470Z", values.get(4).get(1));
        assertEquals("cat", values.get(4).get(0));

        assertEquals("2021-04-29T17:29:22.470Z", values.get(4).get(1));
        assertEquals("cow", values.get(5).get(0));

        assertEquals("2021-04-29T17:29:22.470Z", values.get(4).get(1));
        assertEquals("hamster", values.get(6).get(0));

        assertEquals("2021-04-29T17:29:22.470Z", values.get(4).get(1));
        assertEquals("rat", values.get(7).get(0));
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
}
