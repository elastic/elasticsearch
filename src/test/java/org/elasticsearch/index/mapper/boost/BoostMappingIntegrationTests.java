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

package org.elasticsearch.index.mapper.boost;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;


public class BoostMappingIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_boost")
                .field("store", "yes").field("index", "not_analyzed")
                .endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type", mapping));
        ensureYellow();
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().addIndices("test").addTypes("type").setFields("_boost").get();
        assertTrue(response.mappings().containsKey("test"));
        assertNotNull(response.fieldMappings("test", "type", "_boost"));
        Map<String, Object> boostSource = response.fieldMappings("test", "type", "_boost").sourceAsMap();
        assertThat((Boolean)((LinkedHashMap)(boostSource.get("_boost"))).get("store"), equalTo(true));
        assertThat((String)((LinkedHashMap)(boostSource.get("_boost"))).get("index"), equalTo("not_analyzed"));
    }
}
