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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.index.mapper.DynamicMappingIT.assertMappingsHaveField;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class LegacyDynamicMappingIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testMappingsPropagatedToMasterNodeImmediatelyMultiType() throws Exception {
        assertAcked(prepareCreate("index").setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));
        // allows for multiple types

        // works when the type has been dynamically created
        client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type").get();
            assertMappingsHaveField(mappings, "index", "type", "foo");
        });


        // works if the type already existed
        client().prepareIndex("index", "type", "1").setSource("bar", "baz").get();
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type").get();
            assertMappingsHaveField(mappings, "index", "type", "bar");
        });

        // works if we indexed an empty document
        client().prepareIndex("index", "type2", "1").setSource().get();
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type2").get();
            assertNotNull(mappings.getMappings().get("index"));
            assertTrue(mappings.getMappings().get("index").toString(), mappings.getMappings().get("index").containsKey("type2"));
        });
    }

}
