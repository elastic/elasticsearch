/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper.GeoShapeFieldType;

public class LegacyGeoShapeFieldTypeTests extends FieldTypeTestCase {

    /**
     * Test for {@link LegacyGeoShapeFieldMapper.GeoShapeFieldType#setStrategy(SpatialStrategy)} that checks
     * that {@link LegacyGeoShapeFieldMapper.GeoShapeFieldType#pointsOnly()} gets set as a side effect when using SpatialStrategy.TERM
     */
    public void testSetStrategyName() {
        GeoShapeFieldType fieldType = new GeoShapeFieldType("field");
        assertFalse(fieldType.pointsOnly());
        fieldType.setStrategy(SpatialStrategy.RECURSIVE);
        assertFalse(fieldType.pointsOnly());
        fieldType.setStrategy(SpatialStrategy.TERM);
        assertTrue(fieldType.pointsOnly());
    }
}
