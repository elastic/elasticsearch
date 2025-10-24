/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class UnsupportedAggregationsTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testCardinalityAggregationOnGeoShape() {
        MappedFieldType fieldType = new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType(
            "geometry",
            true,
            true,
            randomBoolean(),
            Orientation.RIGHT,
            null,
            null,
            null,
            false,
            Collections.emptyMap()
        );
        BinaryShapeDocValuesField field = GeoTestUtils.binaryGeoShapeDocValuesField("geometry", new Point(0, 0));
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder("cardinality").field("geometry");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            Document doc = new Document();
            doc.add(field);
            iw.addDocument(doc);
        }, agg -> {}, new AggTestConfig(builder, fieldType)));
        assertThat(exception.getMessage(), equalTo("Field [geometry] of type [geo_shape] is not supported for aggregation [cardinality]"));
    }

    public void testCardinalityAggregationOnShape() {
        MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
            "geometry",
            true,
            true,
            Orientation.RIGHT,
            null,
            false,
            Collections.emptyMap()
        );
        BinaryShapeDocValuesField field = GeoTestUtils.binaryCartesianShapeDocValuesField("geometry", new Point(0, 0));
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder("cardinality").field("geometry");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            Document doc = new Document();
            doc.add(field);
            iw.addDocument(doc);
        }, agg -> {}, new AggTestConfig(builder, fieldType)));
        assertThat(exception.getMessage(), equalTo("Field [geometry] of type [shape] is not supported for aggregation [cardinality]"));
    }

    public void testValueCountAggregationOnGeoShape() {
        MappedFieldType fieldType = new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType(
            "geometry",
            true,
            true,
            randomBoolean(),
            Orientation.RIGHT,
            null,
            null,
            null,
            false,
            Collections.emptyMap()
        );
        BinaryShapeDocValuesField field = GeoTestUtils.binaryGeoShapeDocValuesField("geometry", new Point(0, 0));
        ValueCountAggregationBuilder builder = new ValueCountAggregationBuilder("cardinality").field("geometry");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            Document doc = new Document();
            doc.add(field);
            iw.addDocument(doc);
        }, agg -> {}, new AggTestConfig(builder, fieldType)));
        assertThat(exception.getMessage(), equalTo("Field [geometry] of type [geo_shape] is not supported for aggregation [value_count]"));
    }

    public void testValueCountAggregationOShape() {
        MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
            "geometry",
            true,
            true,
            Orientation.RIGHT,
            null,
            false,
            Collections.emptyMap()
        );
        BinaryShapeDocValuesField field = GeoTestUtils.binaryCartesianShapeDocValuesField("geometry", new Point(0, 0));
        ValueCountAggregationBuilder builder = new ValueCountAggregationBuilder("cardinality").field("geometry");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            Document doc = new Document();
            doc.add(field);
            iw.addDocument(doc);
        }, agg -> {}, new AggTestConfig(builder, fieldType)));
        assertThat(exception.getMessage(), equalTo("Field [geometry] of type [shape] is not supported for aggregation [value_count]"));
    }
}
