/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.startree;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StarTreeConfigTests extends ESTestCase {

    public void testParseValidConfig() {
        Map<String, Object> configMap = new LinkedHashMap<>();

        // Create default star-tree definition
        Map<String, Object> defaultTree = new LinkedHashMap<>();

        // Dimensions
        List<Map<String, Object>> grouping_fields = List.of(
            Map.of("field", "country"),
            Map.of("field", "browser"),
            Map.of("field", "@timestamp", "type", "date_histogram", "interval", "1h")
        );
        defaultTree.put("grouping_fields", grouping_fields);

        // Metrics
        List<Map<String, Object>> values = List.of(
            Map.of("field", "bytes", "aggregations", List.of("SUM", "COUNT", "MIN", "MAX")),
            Map.of("field", "latency", "aggregations", List.of("SUM", "COUNT"))
        );
        defaultTree.put("values", values);

        // Config
        Map<String, Object> config = Map.of("max_leaf_docs", 5000, "star_node_threshold", 8000);
        defaultTree.put("config", config);

        configMap.put("default", defaultTree);

        StarTreeConfig starTreeConfig = StarTreeConfig.parse(configMap);

        assertThat(starTreeConfig.getStarTrees().keySet(), hasSize(1));
        assertTrue(starTreeConfig.hasStarTree("default"));

        StarTreeConfig.StarTreeFieldConfig fieldConfig = starTreeConfig.getStarTree("default");
        assertThat(fieldConfig.getName(), equalTo("default"));
        assertThat(fieldConfig.getGroupingFields(), hasSize(3));
        assertThat(fieldConfig.getValues(), hasSize(2));
        assertThat(fieldConfig.getMaxLeafDocs(), equalTo(5000));
        assertThat(fieldConfig.getStarNodeThreshold(), equalTo(8000));

        // Check grouping_fields
        StarTreeGroupingField dim1 = fieldConfig.getGroupingFields().get(0);
        assertThat(dim1.getField(), equalTo("country"));
        assertThat(dim1.getType(), equalTo(StarTreeGroupingField.GroupingType.ORDINAL));

        StarTreeGroupingField dim3 = fieldConfig.getGroupingFields().get(2);
        assertThat(dim3.getField(), equalTo("@timestamp"));
        assertThat(dim3.getType(), equalTo(StarTreeGroupingField.GroupingType.DATE_HISTOGRAM));
        assertThat(dim3.getInterval(), equalTo(TimeValue.timeValueHours(1)));

        // Check values
        StarTreeValue metric1 = fieldConfig.getValues().get(0);
        assertThat(metric1.getField(), equalTo("bytes"));
        assertTrue(metric1.hasAggregation(StarTreeAggregationType.SUM));
        assertTrue(metric1.hasAggregation(StarTreeAggregationType.COUNT));
        assertTrue(metric1.hasAggregation(StarTreeAggregationType.MIN));
        assertTrue(metric1.hasAggregation(StarTreeAggregationType.MAX));
    }

    public void testParseDefaultConfigValues() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        Map<String, Object> defaultTree = new LinkedHashMap<>();

        defaultTree.put("grouping_fields", List.of(Map.of("field", "country")));
        defaultTree.put("values", List.of(Map.of("field", "bytes", "aggregations", List.of("SUM"))));

        configMap.put("default", defaultTree);

        StarTreeConfig starTreeConfig = StarTreeConfig.parse(configMap);
        StarTreeConfig.StarTreeFieldConfig fieldConfig = starTreeConfig.getStarTree("default");

        assertThat(fieldConfig.getMaxLeafDocs(), equalTo(StarTreeConfig.DEFAULT_MAX_LEAF_DOCS));
        assertThat(fieldConfig.getStarNodeThreshold(), equalTo(StarTreeConfig.DEFAULT_STAR_NODE_THRESHOLD));
    }

    public void testEmptyConfigThrows() {
        Map<String, Object> configMap = new HashMap<>();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> StarTreeConfig.parse(configMap));
        assertThat(e.getMessage(), containsString("cannot be empty"));
    }

    public void testMissingDimensionsThrows() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        Map<String, Object> defaultTree = new LinkedHashMap<>();
        defaultTree.put("values", List.of(Map.of("field", "bytes", "aggregations", List.of("SUM"))));
        configMap.put("default", defaultTree);

        Exception e = expectThrows(IllegalArgumentException.class, () -> StarTreeConfig.parse(configMap));
        assertThat(e.getMessage(), containsString("must have at least one grouping field"));
    }

    public void testMissingMetricsThrows() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        Map<String, Object> defaultTree = new LinkedHashMap<>();
        defaultTree.put("grouping_fields", List.of(Map.of("field", "country")));
        configMap.put("default", defaultTree);

        Exception e = expectThrows(IllegalArgumentException.class, () -> StarTreeConfig.parse(configMap));
        assertThat(e.getMessage(), containsString("must have at least one value"));
    }

    public void testSerialization() throws IOException {
        Map<String, Object> configMap = new LinkedHashMap<>();
        Map<String, Object> defaultTree = new LinkedHashMap<>();
        defaultTree.put("grouping_fields", List.of(Map.of("field", "country")));
        defaultTree.put("values", List.of(Map.of("field", "bytes", "aggregations", List.of("SUM", "COUNT"))));
        configMap.put("default", defaultTree);

        StarTreeConfig original = StarTreeConfig.parse(configMap);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        StarTreeConfig deserialized = new StarTreeConfig(in);

        assertEquals(original, deserialized);
    }

    public void testToXContent() throws IOException {
        Map<String, Object> configMap = new LinkedHashMap<>();
        Map<String, Object> defaultTree = new LinkedHashMap<>();
        defaultTree.put("grouping_fields", List.of(Map.of("field", "country")));
        defaultTree.put("values", List.of(Map.of("field", "bytes", "aggregations", List.of("SUM"))));
        configMap.put("default", defaultTree);

        StarTreeConfig starTreeConfig = StarTreeConfig.parse(configMap);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        starTreeConfig.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = Strings.toString(builder);
        assertTrue("Expected JSON to contain '\"default\"' but got: " + json, json.contains("\"default\""));
        assertTrue(json.contains("\"grouping_fields\""));
        assertTrue(json.contains("\"country\""));
        assertTrue(json.contains("\"values\""));
        assertTrue(json.contains("\"bytes\""));
    }

    public void testMerge() {
        // Create first config
        Map<String, Object> configMap1 = new LinkedHashMap<>();
        Map<String, Object> tree1 = new LinkedHashMap<>();
        tree1.put("grouping_fields", List.of(Map.of("field", "country")));
        tree1.put("values", List.of(Map.of("field", "bytes", "aggregations", List.of("SUM"))));
        configMap1.put("tree1", tree1);
        StarTreeConfig config1 = StarTreeConfig.parse(configMap1);

        // Create second config
        Map<String, Object> configMap2 = new LinkedHashMap<>();
        Map<String, Object> tree2 = new LinkedHashMap<>();
        tree2.put("grouping_fields", List.of(Map.of("field", "browser")));
        tree2.put("values", List.of(Map.of("field", "latency", "aggregations", List.of("COUNT"))));
        configMap2.put("tree2", tree2);
        StarTreeConfig config2 = StarTreeConfig.parse(configMap2);

        // Merge
        StarTreeConfig merged = config1.merge(config2);

        assertTrue(merged.hasStarTree("tree1"));
        assertTrue(merged.hasStarTree("tree2"));
        assertThat(merged.getStarTrees().keySet(), hasSize(2));
    }
}
