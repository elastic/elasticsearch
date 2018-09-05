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

package org.elasticsearch.indices.exists.types;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyTypeExistsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSimple() throws Exception {
        Client client = client();
        CreateIndexResponse response1 = client.admin().indices().prepareCreate("test1")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2").endObject().endObject())
                .execute().actionGet();
        CreateIndexResponse response2 = client.admin().indices().prepareCreate("test2")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
                .execute().actionGet();
        client.admin().indices().prepareAliases().addAlias("test1", "alias1").execute().actionGet();
        assertAcked(response1);
        assertAcked(response2);

        TypesExistsResponse response = client.admin().indices().prepareTypesExists("test1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1").setTypes("type3").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type1").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexNotFoundException e) {}
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type0").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexNotFoundException e) {}
        response = client.admin().indices().prepareTypesExists("alias1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("*").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
    }

}
