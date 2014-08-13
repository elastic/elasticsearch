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

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class GeoMappingTests extends ElasticsearchIntegrationTest {

    public void testUpdatePrecision() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", XContentFactory.jsonBuilder().startObject()
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
                .endObject()).get());
        ensureYellow();
        assertPrecision(new Distance(2, DistanceUnit.MILLIMETERS));

        assertAcked(client().admin().indices().preparePutMapping("test").setType("type1").setSource(XContentFactory.jsonBuilder().startObject()
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
            .endObject()).get());

        assertPrecision(new Distance(11, DistanceUnit.METERS));
    }

    private void assertPrecision(Distance expected) throws Exception {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client().admin().indices().getMappings(new GetMappingsRequest().indices("test").types("type1")).actionGet().getMappings();
        assertNotNull(mappings);
        Map<String, ?> properties = (Map<String, ?>) mappings.get("test").get("type1").getSourceAsMap().get("properties");
        Map<String, ?> pinProperties = (Map<String, ?>) properties.get("pin");
        Map<String, ?> pinFieldData = (Map<String, ?>) pinProperties.get("fielddata");
        Distance precision = Distance.parseDistance(pinFieldData.get("precision").toString());
        assertEquals(expected, precision);
    }

}
