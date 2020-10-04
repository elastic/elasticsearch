/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new GeoPointFieldMapper.Builder("field").build(context).fieldType();

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
