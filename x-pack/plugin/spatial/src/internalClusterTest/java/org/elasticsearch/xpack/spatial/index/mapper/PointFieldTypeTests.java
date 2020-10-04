/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class PointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new PointFieldMapper.Builder("field").build(context).fieldType();

        Map<String, Object> jsonPoint = org.elasticsearch.common.collect.Map.of(
            "type", "Point",
            "coordinates", Arrays.asList(42.0, 27.1));
        String wktPoint = "POINT (42.0 27.1)";
        Map<String, Object> otherJsonPoint = org.elasticsearch.common.collect.Map.of(
            "type", "Point",
            "coordinates", Arrays.asList(30.0, 50.0));
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [x, y] array format.
        Object sourceValue = Arrays.asList(42.0, 27.1);
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in "x, y" string format.
        sourceValue = "42.0,27.1";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [x, y] array format.
        sourceValue = Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0));
        assertEquals(Arrays.asList(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
