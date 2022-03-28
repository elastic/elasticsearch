/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Since ES 8.2, field-caps internal responses are shared between indices that have the same index mapping hash to
 * reduce the transport message size between nodes and clusters, and the memory usage to hold these internal responses.
 * As the optimization is applied for only field-caps requests without index-filter and nodes on 8.2 or later,
 * these BWC tests verify these combinations of field-caps requests: (old|new|mixed indices) and (with|without index filter)
 *
 * In 8.2 we also added the ability to filter fields by type and metadata, with some post-hoc filtering applied on
 * the co-ordinating node if older nodes were included in the system
 */
public class FieldCapsIT extends AbstractRollingTestCase {
    private static boolean indicesCreated = false;

    @Before
    public void setupIndices() throws Exception {
        if (indicesCreated) {
            return;
        }
        indicesCreated = true;
        final String redMapping = """
             "properties": {
               "red_field": { "type": "keyword" },
               "yellow_field": { "type": "integer" },
               "blue_field": { "type": "keyword" },
               "multi_field" : { "type" : "ip", "fields" : { "keyword" : { "type" : "keyword" } } },
               "timestamp": {"type": "date"}
             }
            """;
        final String greenMapping = """
             "properties": {
               "green_field": { "type": "keyword" },
               "yellow_field": { "type": "long" },
               "blue_field": { "type": "keyword" },
               "multi_field" : { "type" : "ip", "fields" : { "keyword" : { "type" : "keyword" } } },
               "timestamp": {"type": "date"}
             }
            """;
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex("old_red_1", Settings.EMPTY, redMapping);
            createIndex("old_red_2", Settings.EMPTY, redMapping);
            createIndex("old_red_empty", Settings.EMPTY, redMapping);
            createIndex("old_green_1", Settings.EMPTY, greenMapping);
            createIndex("old_green_2", Settings.EMPTY, greenMapping);
            createIndex("old_green_empty", Settings.EMPTY, greenMapping);
            for (String index : List.of("old_red_1", "old_red_2", "old_green_1", "old_green_2")) {
                final Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/1");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity(
                    Strings.toString(JsonXContent.contentBuilder().startObject().field("timestamp", "2020-01-01").endObject())
                );
                assertOK(client().performRequest(indexRequest));
            }
        } else if (CLUSTER_TYPE == ClusterType.MIXED && FIRST_MIXED_ROUND) {
            createIndex("new_red_1", Settings.EMPTY, redMapping);
            createIndex("new_red_2", Settings.EMPTY, redMapping);
            createIndex("new_red_empty", Settings.EMPTY, redMapping);
            createIndex("new_green_1", Settings.EMPTY, greenMapping);
            createIndex("new_green_2", Settings.EMPTY, greenMapping);
            createIndex("new_green_empty", Settings.EMPTY, greenMapping);
            for (String index : List.of("new_red_1", "new_red_2", "new_green_1", "new_green_2")) {
                final Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/1");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity(
                    Strings.toString(JsonXContent.contentBuilder().startObject().field("timestamp", "2020-10-10").endObject())
                );
                assertOK(client().performRequest(indexRequest));
            }
        }
    }

    public void testOldIndicesOnly() throws Exception {
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*"), List.of("*"), null, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "old_red_1", "old_red_2", "old_red_empty" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*"), List.of("*"), null, null, null);
            assertThat(
                resp.getIndices(),
                equalTo(new String[] { "old_green_1", "old_green_2", "old_green_empty", "old_red_1", "old_red_2", "old_red_empty" })
            );
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
    }

    public void testOldIndicesWithIndexFilter() throws Exception {
        final QueryBuilder indexFilter = QueryBuilders.rangeQuery("timestamp").gte("2020-01-01").lte("2020-12-12");
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_red_*"), List.of("*"), indexFilter, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "old_red_1", "old_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*"), List.of("*"), indexFilter, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "old_green_1", "old_green_2", "old_red_1", "old_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
    }

    public void testNewIndicesOnly() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("new_red_*"), List.of("*"), null, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "new_red_1", "new_red_2", "new_red_empty" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("new_*"), List.of("*"), null, null, null);
            assertThat(
                resp.getIndices(),
                equalTo(new String[] { "new_green_1", "new_green_2", "new_green_empty", "new_red_1", "new_red_2", "new_red_empty" })
            );
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
    }

    public void testNewIndicesOnlyWithIndexFilter() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        final QueryBuilder indexFilter = QueryBuilders.rangeQuery("timestamp").gte("2020-01-01").lte("2020-12-12");
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("new_red_*"), List.of("*"), indexFilter, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "new_red_1", "new_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
        {
            FieldCapabilitiesResponse resp = fieldCaps(List.of("new_*"), List.of("*"), indexFilter, null, null);
            assertThat(resp.getIndices(), equalTo(new String[] { "new_green_1", "new_green_2", "new_red_1", "new_red_2" }));
            assertThat(resp.getField("red_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("red_field").get("keyword").isSearchable());
            assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
            assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
            assertTrue(resp.getField("yellow_field").get("long").isSearchable());
            assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
            assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
        }
    }

    public void testAllIndices() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*", "new_*"), List.of("*"), null, null, null);
        assertThat(
            resp.getIndices(),
            equalTo(
                new String[] {
                    "new_green_1",
                    "new_green_2",
                    "new_green_empty",
                    "new_red_1",
                    "new_red_2",
                    "new_red_empty",
                    "old_green_1",
                    "old_green_2",
                    "old_green_empty",
                    "old_red_1",
                    "old_red_2",
                    "old_red_empty" }
            )
        );
        assertThat(resp.getField("red_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("red_field").get("keyword").isSearchable());
        assertThat(resp.getField("green_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("green_field").get("keyword").isSearchable());
        assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
        assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        assertTrue(resp.getField("yellow_field").get("long").isSearchable());
        assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
    }

    public void testAllIndicesWithIndexFilter() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        final QueryBuilder indexFilter = QueryBuilders.rangeQuery("timestamp").gte("2020-01-01").lte("2020-12-12");
        FieldCapabilitiesResponse resp = fieldCaps(List.of("old_*", "new_*"), List.of("*"), indexFilter, null, null);
        assertThat(
            resp.getIndices(),
            equalTo(
                new String[] {
                    "new_green_1",
                    "new_green_2",
                    "new_red_1",
                    "new_red_2",
                    "old_green_1",
                    "old_green_2",
                    "old_red_1",
                    "old_red_2" }
            )
        );
        assertThat(resp.getField("red_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("red_field").get("keyword").isSearchable());
        assertThat(resp.getField("green_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("green_field").get("keyword").isSearchable());
        assertThat(resp.getField("yellow_field").keySet(), contains("integer", "long"));
        assertTrue(resp.getField("yellow_field").get("integer").isSearchable());
        assertTrue(resp.getField("yellow_field").get("long").isSearchable());
        assertThat(resp.getField("blue_field").keySet(), contains("keyword"));
        assertTrue(resp.getField("blue_field").get("keyword").isSearchable());
    }

    @SuppressWarnings("unchecked")
    // Returns a client connected to one of the upgraded nodes.
    private RestClient getUpgradedNodeClient() throws IOException {
        for (HttpHost host : getClusterHosts()) {
            RestClient client = RestClient.builder(host).build();
            Request nodesRequest = new Request("GET", "_nodes/_local/_none");
            Map<String, ?> nodeMap = (Map<String, ?>) entityAsMap(client.performRequest(nodesRequest)).get("nodes");
            Map<String, ?> nameMap = (Map<String, ?>) nodeMap.values().iterator().next();
            String version = (String) nameMap.get("version");
            if (version.equals(Version.CURRENT.toString())) {
                return client;
            }
            client.close();
        }
        throw new IllegalStateException("Couldn't find node on version " + Version.CURRENT);
    }

    // Test field type filtering on mixed cluster
    // We need to use a client that is connected to one of the upgraded nodes,
    // because we are testing that the upgraded node will correctly apply filtering
    // to responses from older nodes that don't understand the filter parameters
    public void testAllIndicesWithFieldTypeFilter() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        RestClient restClient = getUpgradedNodeClient();
        FieldCapabilitiesResponse resp = fieldCaps(restClient, List.of("old_*", "new_*"), List.of("*"), null, "keyword", null);
        assertThat(resp.getField("red_field").keySet(), contains("keyword"));
        assertNull(resp.getField("yellow_field"));
        restClient.close();
    }

    // Test multifield exclusion on mixed cluster
    // We need to use a client that is connected to one of the upgraded nodes,
    // because we are testing that the upgraded node will correctly apply filtering
    // to responses from older nodes that don't understand the filter parameters
    public void testAllIndicesWithExclusionFilter() throws Exception {
        assumeFalse("required mixed or upgraded cluster", CLUSTER_TYPE == ClusterType.OLD);
        RestClient client = getUpgradedNodeClient();
        {
            FieldCapabilitiesResponse resp = fieldCaps(client, List.of("old_*", "new_*"), List.of("*"), null, null, null);
            assertThat(resp.getField("multi_field.keyword").keySet(), contains("keyword"));
        }
        {
            FieldCapabilitiesResponse resp = fieldCaps(client, List.of("old_*", "new_*"), List.of("*"), null, null, "-multifield");
            assertThat(resp.getField("multi_field").keySet(), contains("ip"));
            assertNull(resp.getField("multi_field.keyword"));
        }
        client.close();
    }
}
