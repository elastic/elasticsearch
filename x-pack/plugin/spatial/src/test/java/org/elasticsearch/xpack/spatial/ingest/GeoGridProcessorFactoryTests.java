/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.geo.GeometryParserFormat;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GeoGridProcessorFactoryTests extends ESTestCase {

    private GeoGridProcessor.Factory factory;

    @Before
    public void init() {
        factory = new GeoGridProcessor.Factory();
    }

    public void testCreateGeohash() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOHASH.name());
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "field1");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOHASH));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.GEOJSON));
    }

    public void testCreateGeoTile() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOTILE.name());
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "field1");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOTILE));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.GEOJSON));
    }

    public void testCreateGeohex() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOHEX.name());
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "field1");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOHEX));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.GEOJSON));
    }

    public void testCreateInvalidTargetFormat() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOTILE.name());
        config.put("target_format", "invalid");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[target_format] illegal value [invalid], valid values are [WKT, GEOJSON]"));
    }

    public void testCreateMissingField() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithTargetField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "other");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOTILE.name());
        config.put("target_format", "wkt");
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "other");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOTILE));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.WKT));
    }

    public void testCreateWithChildrenField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOHEX.name());
        config.put("children_field", "children");
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "field1", "", "children", "", "");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOHEX));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.GEOJSON));
    }

    public void testCreateWithExtraFields() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", GeoGridProcessor.TileFieldType.GEOHEX.name());
        config.put("parent_field", "parent");
        config.put("children_field", "children");
        config.put("non_children_field", "nonChildren");
        config.put("precision_field", "precision");
        config.put("target_format", "wkt");
        String processorTag = randomAlphaOfLength(10);
        GeoGridProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertFields(processor, "field1", "field1", "parent", "children", "nonChildren", "precision");
        assertThat(processor.tileType(), equalTo(GeoGridProcessor.TileFieldType.GEOHEX));
        assertThat(processor.targetFormat(), equalTo(GeometryParserFormat.WKT));
    }

    public void testCreateWithNoTileTypeDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[tile_type] required property is missing"));
    }

    public void testCreateWithInvalidTileTypeDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tile_type", "super_tiles");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[tile_type] illegal value [super_tiles], valid values are [GEOHASH, GEOTILE, GEOHEX]"));
    }

    private void assertFields(GeoGridProcessor processor, String field, String targetField) {
        assertFields(processor, field, targetField, "", "", "", "");
    }

    private void assertFields(
        GeoGridProcessor processor,
        String field,
        String targetField,
        String parentField,
        String childrenField,
        String nonChildrenField,
        String precisionField
    ) {
        assertThat(processor.field("field"), equalTo(field));
        assertThat(processor.field("target_field"), equalTo(targetField));
        assertThat(processor.field("parent_field"), equalTo(parentField));
        assertThat(processor.field("children_field"), equalTo(childrenField));
        assertThat(processor.field("non_children_field"), equalTo(nonChildrenField));
        assertThat(processor.field("precision_field"), equalTo(precisionField));
    }
}
