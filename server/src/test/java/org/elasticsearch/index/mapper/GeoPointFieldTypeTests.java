/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new GeoPointFieldMapper.Builder("field", false).build(new ContentPath()).fieldType();

        Map<String, Object> jsonPoint = new HashMap<>();
        jsonPoint.put("type", "Point");
        jsonPoint.put("coordinates", Arrays.asList(42.0, 27.1));
        Map<String, Object> otherJsonPoint = new HashMap<>();
        otherJsonPoint.put("type", "Point");
        otherJsonPoint.put("coordinates", Arrays.asList(30.0, 50.0));
        String wktPoint = "POINT (42.0 27.1)";
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [lon, lat] array format.
        Object sourceValue = Arrays.asList(42.0, 27.1);
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in "lat, lon" string format.
        sourceValue = "27.1,42.0";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [lon, lat] array format.
        sourceValue = Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0));
        assertEquals(Arrays.asList(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
