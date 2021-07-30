/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CircleProcessorFactoryTests extends ESTestCase {

    private CircleProcessor.Factory factory;

    @Before
    public void init() {
        factory = new CircleProcessor.Factory();
    }

    public void testCreateGeoShape() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("error_distance", 0.002);
        config.put("shape_type", "geo_shape");
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.field(), equalTo("field1"));
        assertThat(processor.targetField(), equalTo("field1"));
        assertThat(processor.errorDistance(), equalTo(0.002));
        assertThat(processor.shapeType(), equalTo(CircleProcessor.CircleShapeFieldType.GEO_SHAPE));
    }

    public void testCreateShape() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("error_distance", 0.002);
        config.put("shape_type", "shape");
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.field(), equalTo("field1"));
        assertThat(processor.targetField(), equalTo("field1"));
        assertThat(processor.errorDistance(), equalTo(0.002));
        assertThat(processor.shapeType(), equalTo(CircleProcessor.CircleShapeFieldType.SHAPE));
    }

    public void testCreateInvalidShapeType() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("error_distance", 0.002);
        config.put("shape_type", "invalid");
        String processorTag = randomAlphaOfLength(10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(e.getMessage(), equalTo("illegal [shape_type] value [invalid]. valid values are [SHAPE, GEO_SHAPE]"));
    }

    public void testCreateMissingField() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithTargetField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "other");
        config.put("error_distance", 0.002);
        config.put("shape_type", "geo_shape");
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.field(), equalTo("field1"));
        assertThat(processor.targetField(), equalTo("other"));
        assertThat(processor.errorDistance(), equalTo(0.002));
        assertThat(processor.shapeType(), equalTo(CircleProcessor.CircleShapeFieldType.GEO_SHAPE));
    }

    public void testCreateWithNoErrorDistanceDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config));
        assertThat(e.getMessage(), equalTo("[error_distance] required property is missing"));
    }
}
