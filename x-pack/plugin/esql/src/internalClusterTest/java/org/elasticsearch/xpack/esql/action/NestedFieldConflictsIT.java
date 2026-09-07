/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-index nested-vs-object type skew (#154011): field caps filters {@code -nested},
 * so the coordinator plans the object type and the nested shard must contribute nulls.
 * If ES|QL later supports nested fields, these expectations will need updating.
 * <p>
 *     Each scenario has a {@code SameNode} sibling that pins both indices to the
 *     same node.
 * </p>
 */
public class NestedFieldConflictsIT extends AbstractEsqlIntegTestCase {

    /**
     * {@code item.value} is {@code integer} under nested in one index and
     * {@code long} under a plain object in another.
     */
    public void testIntegerVsLong() {
        testIntegerVsLong(false);
    }

    /** Same as {@link #testIntegerVsLong()} but both indices on the same node. */
    public void testIntegerVsLongSameNode() {
        testIntegerVsLong(true);
    }

    public void testDoubleVsLong() {
        testDoubleVsLong(false);
    }

    public void testDoubleVsLongSameNode() {
        testDoubleVsLong(true);
    }

    public void testKeywordVsDate() {
        testKeywordVsDate(false);
    }

    public void testKeywordVsDateSameNode() {
        testKeywordVsDate(true);
    }

    /**
     * Same type on both indices, but the nested mapping copies values onto the root.
     * Pre-fix this leaked nested values into ES|QL; they must still be null.
     */
    public void testIncludeInRootSameType() {
        testIncludeInRootSameType(false);
    }

    public void testIncludeInRootSameTypeSameNode() {
        testIncludeInRootSameType(true);
    }

    /**
     * A nested-only index hides {@code item.value} from field caps, so the coordinator treats it as
     * fully unmapped. {@code NULLIFY} lets the query run; the nested shard still contributes nulls.
     */
    public void testUnmappedFieldsNullifySingleNestedIndex() {
        assumeTrue("Requires SET unmapped_fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        String nested = createSingleNestedIndex();
        assertThat(
            esql("SET unmapped_fields=\"nullify\"; FROM " + nested + " | KEEP id, item.value | SORT id"),
            equalTo(List.of(Arrays.asList("n00", null), Arrays.asList("n01", null)))
        );
        assertThat(esql("SET unmapped_fields=\"nullify\"; FROM " + nested + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(0L))));
    }

    /**
     * {@code LOAD} would normally read a fully unmapped field from {@code _source}. Nested subfields
     * stay null so the loader cannot reopen the typed nested mapping.
     */
    public void testUnmappedFieldsLoadSingleNestedIndex() {
        assumeTrue("Requires unmapped_fields=\"load\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        String nested = createSingleNestedIndex();
        assertThat(
            esql("SET unmapped_fields=\"load\"; FROM " + nested + " | KEEP id, item.value | SORT id"),
            equalTo(List.of(Arrays.asList("n00", null), Arrays.asList("n01", null)))
        );
        assertThat(esql("SET unmapped_fields=\"load\"; FROM " + nested + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(0L))));
    }

    private void testIntegerVsLong(boolean sameNode) {
        String[] nodes = pinNodes(sameNode);
        String nested = "nest_int_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_long_" + getTestName().toLowerCase(Locale.ROOT);
        createPinnedIndex(nested, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "type": "nested", "properties": { "value": { "type": "integer" } } } } }""", nodes[0]);
        createPinnedIndex(object, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "properties": { "value": { "type": "long" } } } } }""", nodes[1]);
        for (int i = 0; i < 20; i++) {
            indexJson(nested, Integer.toString(i), Strings.format("""
                {"id": "n%02d", "item": [{"value": %d}]}""", i, i + 100));
            indexJson(object, Integer.toString(i), Strings.format("""
                {"id": "o%02d", "item": {"value": %d}}""", i, i + 1));
        }
        refresh(nested, object);

        String from = "FROM " + nested + ", " + object;
        String value = itemValue();
        assertThat(esql(from + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"), equalTo(List.of(List.of(210L, 20L))));
        assertThat(esql(from + " | KEEP id, " + value + " | SORT id | LIMIT 5"), equalTo(firstFiveNullValueRows()));
    }

    private void testDoubleVsLong(boolean sameNode) {
        String[] nodes = pinNodes(sameNode);
        String nested = "nest_dbl_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_lng_" + getTestName().toLowerCase(Locale.ROOT);
        createPinnedIndex(nested, """
            { "properties": { "item": { "type": "nested", "properties": { "value": { "type": "double" } } } } }""", nodes[0]);
        createPinnedIndex(object, """
            { "properties": { "item": { "properties": { "value": { "type": "long" } } } } }""", nodes[1]);
        for (int i = 0; i < 20; i++) {
            indexJson(nested, Integer.toString(i), Strings.format("""
                {"item": [{"value": %s}]}""", (i + 1) + 0.5));
            indexJson(object, Integer.toString(i), Strings.format("""
                {"item": {"value": %d}}""", i + 1));
        }
        refresh(nested, object);

        String value = itemValue();
        assertThat(
            esql("FROM " + nested + ", " + object + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"),
            equalTo(List.of(List.of(210L, 20L)))
        );
    }

    private void testKeywordVsDate(boolean sameNode) {
        String[] nodes = pinNodes(sameNode);
        String nested = "nest_kw_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_dt_" + getTestName().toLowerCase(Locale.ROOT);
        createPinnedIndex(nested, """
            { "properties": { "item": { "type": "nested", "properties": { "value": { "type": "keyword" } } } } }""", nodes[0]);
        createPinnedIndex(object, """
            { "properties": { "item": { "properties": { "value": { "type": "date" } } } } }""", nodes[1]);
        for (int i = 0; i < 20; i++) {
            indexJson(nested, Integer.toString(i), Strings.format("""
                {"item": [{"value": "nested-%d"}]}""", i));
            indexJson(object, Integer.toString(i), Strings.format("""
                {"item": {"value": "2024-01-%02dT00:00:00.000Z"}}""", i + 1));
        }
        refresh(nested, object);

        assertThat(esql("FROM " + nested + ", " + object + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(20L))));
    }

    private void testIncludeInRootSameType(boolean sameNode) {
        String[] nodes = pinNodes(sameNode);
        String nested = "nest_root_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_root_" + getTestName().toLowerCase(Locale.ROOT);
        // Single-level nested: include_in_parent and include_in_root both copy onto the root.
        String include = randomFrom("include_in_root", "include_in_parent");
        createPinnedIndex(nested, Strings.format("""
            { "properties": { "id": { "type": "keyword" }, "item": {
              "type": "nested", "%s": true, "properties": { "value": { "type": "long" } } } } }""", include), nodes[0]);
        createPinnedIndex(object, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "properties": { "value": { "type": "long" } } } } }""", nodes[1]);
        for (int i = 0; i < 20; i++) {
            indexJson(nested, Integer.toString(i), Strings.format("""
                {"id": "n%02d", "item": [{"value": %d}]}""", i, i + 100));
            indexJson(object, Integer.toString(i), Strings.format("""
                {"id": "o%02d", "item": {"value": %d}}""", i, i + 1));
        }
        refresh(nested, object);

        String from = "FROM " + nested + ", " + object;
        String value = itemValue();
        // Pre-fix leaked nested 100..119 and summed to 2400 with count 40.
        assertThat(esql(from + " | STATS s = SUM(" + value + "), c = COUNT(" + value + ")"), equalTo(List.of(List.of(210L, 20L))));
        // COUNT-only is Lucene EXISTS pushdown (EsStatsQueryExec). Leave the field uncast so
        // this still hits that path. The include flag copies nested values onto the parent
        // doc, so skipping the nested shard is required.
        assertThat(esql(from + " | STATS c = COUNT(item.value)"), equalTo(List.of(List.of(20L))));
        assertThat(esql(from + " | KEEP id, " + value + " | SORT id | LIMIT 5"), equalTo(firstFiveNullValueRows()));
    }

    /**
     * Nested is hidden from field caps, so {@code item.value} is the object index's {@code long},
     * not a union type. A {@code ::long} cast must not start reading nested values.
     */
    private static String itemValue() {
        return randomBoolean() ? "item.value::long" : "item.value";
    }

    private String[] pinNodes(boolean sameNode) {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String node1 = randomDataNode().getName();
        String node2 = sameNode ? node1 : randomValueOtherThan(node1, () -> randomDataNode().getName());
        return new String[] { node1, node2 };
    }

    private String createSingleNestedIndex() {
        String nested = "nest_only_" + getTestName().toLowerCase(Locale.ROOT);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(nested)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("""
                    { "properties": { "id": { "type": "keyword" }, "item": {
                      "type": "nested", "properties": { "value": { "type": "integer" } } } } }""")
        );
        indexJson(nested, "0", """
            {"id": "n00", "item": [{"value": 100}]}""");
        indexJson(nested, "1", """
            {"id": "n01", "item": [{"value": 101}]}""");
        refresh(nested);
        return nested;
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
        ensureGreen(index);
    }

    private void indexJson(String index, String id, String json) {
        client().prepareIndex(index).setId(id).setSource(json, XContentType.JSON).get();
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
