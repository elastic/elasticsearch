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
package org.elasticsearch.index.mapper.geo;

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

public class GeoShapeFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        GeoShapeFieldMapper.GeoShapeFieldType gft = new GeoShapeFieldMapper.GeoShapeFieldType();
        gft.setNames(new MappedFieldType.Names("testgeoshape"));
        return gft;
    }

    @Override
    protected int numProperties() {
        return 6 + super.numProperties();
    }

    @Override
    protected void modifyProperty(MappedFieldType ft, int propNum) {
        GeoShapeFieldMapper.GeoShapeFieldType gft = (GeoShapeFieldMapper.GeoShapeFieldType)ft;
        switch (propNum) {
            case 0: gft.setTree("quadtree"); break;
            case 1: gft.setStrategyName("term"); break;
            case 2: gft.setTreeLevels(10); break;
            case 3: gft.setPrecisionInMeters(20); break;
            case 4: gft.setDefaultDistanceErrorPct(0.5); break;
            case 5: gft.setOrientation(ShapeBuilder.Orientation.LEFT); break;
            default: super.modifyProperty(ft, propNum - 6);
        }
    }
}
