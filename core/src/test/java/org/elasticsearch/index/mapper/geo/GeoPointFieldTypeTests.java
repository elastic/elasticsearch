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

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new GeoPointFieldMapper.GeoPointFieldType();
    }

    @Override
    protected int numProperties() {
        return 6 + super.numProperties();
    }

    @Override
    protected void modifyProperty(MappedFieldType ft, int propNum) {
        GeoPointFieldMapper.GeoPointFieldType gft = (GeoPointFieldMapper.GeoPointFieldType)ft;
        switch (propNum) {
            case 0: gft.setGeohashEnabled(new MappedFieldType(), 1, true); break;
            case 1: gft.setLatLonEnabled(new MappedFieldType(), new MappedFieldType()); break;
            case 2: gft.setValidateLon(!gft.validateLon()); break;
            case 3: gft.setValidateLat(!gft.validateLat()); break;
            case 4: gft.setNormalizeLon(!gft.normalizeLon()); break;
            case 5: gft.setNormalizeLat(!gft.normalizeLat()); break;
            default: super.modifyProperty(ft, numProperties() - propNum - 1);
        }
    }
}
