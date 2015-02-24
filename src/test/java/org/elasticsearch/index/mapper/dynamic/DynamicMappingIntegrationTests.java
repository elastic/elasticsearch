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
package org.elasticsearch.index.mapper.dynamic;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class DynamicMappingIntegrationTests extends ElasticsearchIntegrationTest {

    // https://github.com/elasticsearch/elasticsearch/issues/8423#issuecomment-64229717
    @Test
    public void testStrictAllMapping() throws Exception {
        String defaultMapping = jsonBuilder().startObject().startObject("_default_")
                .field("dynamic", "strict")
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("index").addMapping("_default_", defaultMapping).get();

        try {
            client().prepareIndex("index", "type", "id").setSource("test", "test").get();
            fail();
        } catch (StrictDynamicMappingException ex) {
            // this should not be created dynamically so we expect this exception
        }
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                GetMappingsResponse currentMapping = client().admin().indices().prepareGetMappings("index").get();
                return currentMapping.getMappings().get("index").get("type") != null;
            }
        });

        String docMapping = jsonBuilder().startObject().startObject("type")
                .startObject("_all")
                .field("enabled", false)
                .endObject()
                .endObject().endObject().string();
        try {
            client().admin().indices()
                    .preparePutMapping("index")
                    .setType("type")
                    .setSource(docMapping).get();
            fail();
        } catch (Exception e) {
            // the mapping was created anyway with _all enabled: true, although the index request fails so we expect the update to fail
        }

        // make sure type was created
        for (Client client : cluster()) {
            GetMappingsResponse mapping = client.admin().indices().prepareGetMappings("index").setLocal(true).get();
            assertNotNull(mapping.getMappings().get("index").get("type"));
        }
    }
}
