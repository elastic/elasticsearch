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

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;

import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.verify;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

public class LegacyIndicesOptionsIntegrationIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testPutMappingMultiType() throws Exception {
        assertTrue("remove this multi type test", Version.CURRENT.before(Version.fromString("7.0.0")));
        verify(client().admin().indices().preparePutMapping("foo").setType("type1").setSource("field", "type=text"), true);
        verify(client().admin().indices().preparePutMapping("_all").setType("type1").setSource("field", "type=text"), true);

        for (String index : Arrays.asList("foo", "foobar", "bar", "barbaz")) {
            assertAcked(prepareCreate(index).setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));
            // allows for multiple types
        }

        verify(client().admin().indices().preparePutMapping("foo").setType("type1").setSource("field", "type=text"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type1"), notNullValue());
        verify(client().admin().indices().preparePutMapping("b*").setType("type1").setSource("field", "type=text"), false);
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type1"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type1"), notNullValue());
        verify(client().admin().indices().preparePutMapping("_all").setType("type2").setSource("field", "type=text"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("foobar").get().mappings().get("foobar").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type2"), notNullValue());
        verify(client().admin().indices().preparePutMapping().setType("type3").setSource("field", "type=text"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("foobar").get().mappings().get("foobar").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type3"), notNullValue());


        verify(client().admin().indices().preparePutMapping("c*").setType("type1").setSource("field", "type=text"), true);

        assertAcked(client().admin().indices().prepareClose("barbaz").get());
        verify(client().admin().indices().preparePutMapping("barbaz").setType("type4").setSource("field", "type=text"), false);
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type4"), notNullValue());
    }

}
