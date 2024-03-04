/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * A dedicated test suite for testing time series esql functionality.
 * This while the functionality is gated behind a query pragma.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class TSDBRestEsqlIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTimeSeriesQuerying() throws IOException {
        assertTrue("time series querying relies on query pragma", Build.current().isSnapshot());
        var settings = Settings.builder()
            .loadFromStream("tsdb-settings.json", TSDBRestEsqlIT.class.getResourceAsStream("/tsdb-settings.json"), false)
            .build();
        String mapping = CsvTestsDataLoader.readTextFile(TSDBRestEsqlIT.class.getResource("/tsdb-mapping.json"));
        createIndex("k8s", settings, mapping);

        Request bulk = new Request("POST", "/k8s/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");

        String bulkBody = new String(
            TSDBRestEsqlIT.class.getResourceAsStream("/tsdb-bulk-request.txt").readAllBytes(),
            StandardCharsets.UTF_8
        );
        bulk.setJsonEntity(bulkBody);
        Response response = client().performRequest(bulk);
        assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        RestEsqlTestCase.RequestObjectBuilder builder = new RestEsqlTestCase.RequestObjectBuilder().query(
            "FROM k8s | KEEP k8s.pod.name, @timestamp"
        );
        builder.pragmas(Settings.builder().put("time_series", true).build());
        Map<String, Object> result = runEsqlSync(builder);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> columns = (List<Map<?, ?>>) result.get("columns");
        assertEquals(2, columns.size());
        assertEquals("k8s.pod.name", columns.get(0).get("name"));
        assertEquals("@timestamp", columns.get(1).get("name"));

        // Note that _tsid is a hashed value, so tsid no longer is sorted lexicographically.
        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals(8, values.size());
        assertEquals("hamster", values.get(0).get(0));
        assertEquals("2021-04-29T17:29:22.470Z", values.get(0).get(1));
        assertEquals("hamster", values.get(1).get(0));
        assertEquals("2021-04-29T17:29:12.470Z", values.get(1).get(1));

        assertEquals("rat", values.get(2).get(0));
        assertEquals("2021-04-29T17:29:22.470Z", values.get(2).get(1));
        assertEquals("rat", values.get(3).get(0));
        assertEquals("2021-04-29T17:29:12.470Z", values.get(3).get(1));

        assertEquals("cow", values.get(4).get(0));
        assertEquals("2021-04-29T17:29:22.470Z", values.get(4).get(1));
        assertEquals("cow", values.get(5).get(0));
        assertEquals("2021-04-29T17:29:12.470Z", values.get(5).get(1));

        assertEquals("cat", values.get(6).get(0));
        assertEquals("2021-04-29T17:29:22.470Z", values.get(6).get(1));
        assertEquals("cat", values.get(7).get(0));
        assertEquals("2021-04-29T17:29:12.470Z", values.get(7).get(1));
    }
}
