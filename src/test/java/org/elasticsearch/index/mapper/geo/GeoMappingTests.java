/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.Map;

public class GeoMappingTests extends ElasticsearchIntegrationTest {

    public void testUpdatePrecision() throws Exception {
        prepareCreate("test").addMapping("type1", XContentFactory.jsonBuilder().startObject()
                .startObject("type1")
                    .startObject("properties")
                        .startObject("pin")
                            .field("type", "geo_point")
                            .startObject("fielddata")
                                .field("format", "compressed")
                                .field("precision", "2mm")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()).execute().actionGet();
        ensureYellow();
        assertPrecision(new Distance(2, DistanceUnit.MILLIMETERS));

        client().admin().indices().preparePutMapping("test").setType("type1").setSource(XContentFactory.jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                    .startObject("pin")
                        .field("type", "geo_point")
                            .startObject("fielddata")
                                .field("format", "compressed")
                                .field("precision", "11m")
                            .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject()).execute().actionGet();

        assertPrecision(new Distance(11, DistanceUnit.METERS));
    }

    private void assertPrecision(Distance expected) throws Exception {
        FieldMappingMetaData mappings = client().admin().indices().getFieldMappings(new GetFieldMappingsRequest().indices("test").types("type1").fields("pin")).actionGet().fieldMappings("test", "type1", "pin");
        assertNotNull(mappings);
        Map<String, ?> pinProperties = (Map<String, ?>) mappings.sourceAsMap().get("pin");
        Map<String, ?> pinFieldData = (Map<String, ?>) pinProperties.get("fielddata");
        Distance precision = Distance.parseDistance(pinFieldData.get("precision").toString(), DistanceUnit.METERS);
        assertEquals(expected, precision);
    }

}
