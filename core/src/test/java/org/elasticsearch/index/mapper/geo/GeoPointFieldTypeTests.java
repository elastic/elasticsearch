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
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.junit.Before;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new GeoPointFieldMapper.GeoPointFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("geohash", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((GeoPointFieldMapper.GeoPointFieldType)ft).setGeohashEnabled(new StringFieldMapper.StringFieldType(), 1, true);
            }
        });
        addModifier(new Modifier("lat_lon", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((GeoPointFieldMapper.GeoPointFieldType)ft).setLatLonEnabled(new DoubleFieldMapper.DoubleFieldType(), new DoubleFieldMapper.DoubleFieldType());
            }
        });
    }
}
