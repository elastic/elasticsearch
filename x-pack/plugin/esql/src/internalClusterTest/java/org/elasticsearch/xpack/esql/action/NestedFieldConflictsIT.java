/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-index nested-vs-object type skew (#154011): {@code IndexResolver} applies
 * {@code -nested} on the field-caps request, so the coordinator plans the object type
 * and the nested shard must contribute nulls.
 * If ES|QL later supports nested fields, these expectations will need updating.
 * <p>
 *     Each scenario randomly pins both indices to the same node or to different nodes.
 * </p>
 */
public class NestedFieldConflictsIT extends AbstractEsqlIntegTestCase {

    /**
     * {@code item.value} is {@code integer} under nested in one index and
     * {@code long} under a plain object in another.
     */
    public void testIntegerVsLong() {
        String[] nodes = pinNodes();
        String nested = indexName("nest_int_");
        String object = indexName("obj_long_");
        createPinnedIndex(nested, """
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "type": "nested",
                  "properties": {
                    "value": {
                      "type": "integer"
                    }
                  }
                }
              }
            }""", nodes[0]);
        createPinnedIndex(object, """
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "properties": {
                    "value": {
                      "type": "long"
                    }
                  }
                }
              }
            }""", nodes[1]);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndexJson(nested, Integer.toString(i), Strings.format("""
                {
                  "id": "n%02d",
                  "item": [
                    {
                      "value": %d
                    }
                  ]
                }""", i, i + 100)));
            bulk.add(prepareIndexJson(object, Integer.toString(i), Strings.format("""
                {
                  "id": "o%02d",
                  "item": {
                    "value": %d
                  }
                }""", i, i + 1)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        String from = "FROM " + nested + ", " + object;
        String value = itemValue();
        assertThat(esql(from + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"), equalTo(List.of(List.of(210L, 20L))));
        // KEEP does not accept casts; EVAL covers the convert-on-extract path.
        assertThat(esql(from + " | " + keepIdAndValue() + " | SORT id | LIMIT 5"), equalTo(firstFiveNullValueRows()));
    }

    public void testDoubleVsLong() {
        String[] nodes = pinNodes();
        String nested = indexName("nest_dbl_");
        String object = indexName("obj_lng_");
        createPinnedIndex(nested, """
            {
              "properties": {
                "item": {
                  "type": "nested",
                  "properties": {
                    "value": {
                      "type": "double"
                    }
                  }
                }
              }
            }""", nodes[0]);
        createPinnedIndex(object, """
            {
              "properties": {
                "item": {
                  "properties": {
                    "value": {
                      "type": "long"
                    }
                  }
                }
              }
            }""", nodes[1]);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndexJson(nested, Integer.toString(i), Strings.format("""
                {
                  "item": [
                    {
                      "value": %s
                    }
                  ]
                }""", (i + 1) + 0.5)));
            bulk.add(prepareIndexJson(object, Integer.toString(i), Strings.format("""
                {
                  "item": {
                    "value": %d
                  }
                }""", i + 1)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        String value = itemValue();
        assertThat(
            esql("FROM " + nested + ", " + object + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"),
            equalTo(List.of(List.of(210L, 20L)))
        );
    }

    public void testKeywordVsDate() {
        String[] nodes = pinNodes();
        String nested = indexName("nest_kw_");
        String object = indexName("obj_dt_");
        createPinnedIndex(nested, """
            {
              "properties": {
                "item": {
                  "type": "nested",
                  "properties": {
                    "value": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""", nodes[0]);
        createPinnedIndex(object, """
            {
              "properties": {
                "item": {
                  "properties": {
                    "value": {
                      "type": "date"
                    }
                  }
                }
              }
            }""", nodes[1]);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndexJson(nested, Integer.toString(i), Strings.format("""
                {
                  "item": [
                    {
                      "value": "nested-%d"
                    }
                  ]
                }""", i)));
            bulk.add(prepareIndexJson(object, Integer.toString(i), Strings.format("""
                {
                  "item": {
                    "value": "2024-01-%02dT00:00:00.000Z"
                  }
                }""", i + 1)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertThat(esql("FROM " + nested + ", " + object + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(20L))));
    }

    /**
     * Same type on both indices, but the nested mapping copies values onto the root.
     * Pre-fix this leaked nested values into ES|QL; they must still be null.
     */
    public void testIncludeInRootSameType() {
        String[] nodes = pinNodes();
        String nested = indexName("nest_root_");
        String object = indexName("obj_root_");
        // Single-level nested: include_in_parent and include_in_root both copy onto the root.
        String include = randomFrom("include_in_root", "include_in_parent");
        createPinnedIndex(nested, Strings.format("""
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "type": "nested",
                  "%s": true,
                  "properties": {
                    "value": {
                      "type": "long"
                    }
                  }
                }
              }
            }""", include), nodes[0]);
        createPinnedIndex(object, """
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "properties": {
                    "value": {
                      "type": "long"
                    }
                  }
                }
              }
            }""", nodes[1]);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 20; i++) {
            bulk.add(prepareIndexJson(nested, Integer.toString(i), Strings.format("""
                {
                  "id": "n%02d",
                  "item": [
                    {
                      "value": %d
                    }
                  ]
                }""", i, i + 100)));
            bulk.add(prepareIndexJson(object, Integer.toString(i), Strings.format("""
                {
                  "id": "o%02d",
                  "item": {
                    "value": %d
                  }
                }""", i, i + 1)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        String from = "FROM " + nested + ", " + object;
        String value = itemValue();
        // Pre-fix leaked nested 100..119 and summed to 2400 with count 40.
        assertThat(esql(from + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"), equalTo(List.of(List.of(210L, 20L))));
        // COUNT-only is Lucene EXISTS pushdown (EsStatsQueryExec). Leave the field uncast so
        // this still hits that path. The include flag copies nested values onto the parent
        // doc, so skipping the nested shard is required.
        assertThat(esql(from + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(20L))));
        assertThat(esql(from + " | " + keepIdAndValue() + " | SORT id | LIMIT 5"), equalTo(firstFiveNullValueRows()));
    }

    /**
     * Nested is hidden from field caps, so {@code item.value} is the object index's {@code long},
     * not a union type. A {@code ::long} cast must not start reading nested values.
     */
    private static String itemValue() {
        return randomBoolean() ? "item.value::long" : "item.value";
    }

    /**
     * {@code KEEP} only accepts names and wildcards, not {@code ::} casts.
     */
    private static String keepIdAndValue() {
        return randomBoolean() ? "EVAL v = item.value::long | KEEP id, v" : "KEEP id, item.value";
    }

    private static String indexName(String prefix) {
        return prefix + randomIdentifier();
    }

    private String[] pinNodes() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String node1 = randomDataNode().getName();
        String node2 = randomBoolean() ? node1 : randomValueOtherThan(node1, () -> randomDataNode().getName());
        return new String[] { node1, node2 };
    }

    private void createPinnedIndex(String index, String mapping, String node) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.require._name", node)
                )
                .setMapping(mapping)
        );
    }

    private IndexRequestBuilder prepareIndexJson(String index, String id, String json) {
        return client().prepareIndex(index).setId(id).setSource(json, XContentType.JSON);
    }

    private static List<List<Object>> firstFiveNullValueRows() {
        return List.of(
            Arrays.asList("n00", null),
            Arrays.asList("n01", null),
            Arrays.asList("n02", null),
            Arrays.asList("n03", null),
            Arrays.asList("n04", null)
        );
    }

    private List<List<Object>> esql(String query) {
        try (var resp = run(query)) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            return getValuesList(resp);
        }
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
