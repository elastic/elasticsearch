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
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
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

    private static final String MAPPING = """
        {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "metricset": {
                    "type": "keyword",
                    "time_series_dimension": true
                },
                "k8s": {
                    "properties": {
                        "pod": {
                            "properties": {
                                "uid": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                },
                                "name": {
                                    "type": "keyword"
                                },
                                "cpu": {
                                    "properties": {
                                        "limit": {
                                            "type": "scaled_float",
                                            "scaling_factor": 1000.0,
                                            "time_series_metric": "gauge"
                                        },
                                        "nanocores": {
                                            "type": "long",
                                            "time_series_metric": "gauge"
                                        },
                                        "node": {
                                            "type": "scaled_float",
                                            "scaling_factor": 1000.0,
                                            "time_series_metric": "gauge"
                                        }
                                    }
                                },
                                "network": {
                                    "properties": {
                                        "rx": {
                                             "type": "long",
                                            "time_series_metric": "gauge"
                                        },
                                        "tx": {
                                            "type": "long",
                                            "time_series_metric": "gauge"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """;

    private static final String SETTINGS = """
        {
             "index": {
                 "mode": "time_series",
                 "routing_path": [
                     "metricset",
                     "k8s.pod.uid"
                 ]
             }
         }
        """;

    private static final String BULK =
        """
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:12.470Z", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "network": {"tx": 2001818691, "rx": 802133794},"cpu": {"limit": 0.3787411612903226, "nanocores": 35222928, "node": 0.048845732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:12.470Z", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "uid":"947e4ced-1786-4e53-9e0c-5c447e959508", "network": {"tx": 2005177954, "rx": 801479970},"cpu": {"limit": 0.5786461612903226, "nanocores": 25222928, "node": 0.505805732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:12.470Z", "metricset": "pod", "k8s": {"pod": {"name": "cow", "uid":"947e4ced-1786-4e53-9e0c-5c447e959509", "network": {"tx": 2006223737, "rx": 802337279},"cpu": {"limit": 0.5787451612903226, "nanocores": 55252928, "node": 0.606805732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:12.470Z", "metricset": "pod", "k8s": {"pod": {"name": "rat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959510", "network": {"tx": 2012916202, "rx": 803685721},"cpu": {"limit": 0.6786461612903226, "nanocores": 75227928, "node": 0.058855732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:22.470Z", "metricset": "pod", "k8s": {"pod": {"name": "rat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959510", "network": {"tx": 1434521831, "rx": 530575198},"cpu": {"limit": 0.7787411712903226, "nanocores": 75727928, "node": 0.068865732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:22.470Z", "metricset": "pod", "k8s": {"pod": {"name": "cow", "uid":"947e4ced-1786-4e53-9e0c-5c447e959509", "network": {"tx": 1434577921, "rx": 530600088},"cpu": {"limit": 0.2782412612903226, "nanocores": 25222228, "node": 0.078875732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:22.470Z", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "uid":"947e4ced-1786-4e53-9e0c-5c447e959508", "network": {"tx": 1434587694, "rx": 530604797},"cpu": {"limit": 0.1717411612903226, "nanocores": 15121928, "node": 0.808805732}}}}
            {"create": {}}
            {"@timestamp": "2021-04-29T17:29:22.470Z", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "network": {"tx": 1434595272, "rx": 530605511},"cpu": {"limit": 0.8787481682903226, "nanocores": 95292928, "node": 0.908905732}}}}
            """;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @After
    public void cleanup() throws IOException {
        deleteIndex("_all");
    }

    public void testTimeSeriesQuerying() throws IOException {
        assertTrue("time series querying relies on query pragma", Build.current().isSnapshot());
        var settings = Settings.builder().loadFromSource(SETTINGS, XContentType.JSON).build();
        createIndex("k8s", settings, MAPPING);

        Request bulk = new Request("POST", "/k8s/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(BULK);
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
