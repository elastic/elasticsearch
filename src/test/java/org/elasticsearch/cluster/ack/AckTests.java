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

package org.elasticsearch.cluster.ack;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import static org.elasticsearch.test.AbstractIntegrationTest.Scope.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = SUITE)
public class AckTests extends AbstractIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //to test that the acknowledgement mechanism is working we better disable the wait for publish
        //otherwise the operation is most likely acknowledged even if it doesn't support ack
        return ImmutableSettings.builder().put("discovery.zen.publish_timeout", 0).build();
    }

    @Test
    public void testUpdateSettingsAcknowledgement() {
        createIndex("test");

        UpdateSettingsResponse updateSettingsResponse = client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put("refresh_interval", 9999)).get();
        assertThat(updateSettingsResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
            String refreshInterval = clusterStateResponse.getState().metaData().index("test").settings().get("index.refresh_interval");
            assertThat(refreshInterval, equalTo("9999"));
        }
    }

    @Test
    public void testPutWarmerAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            GetWarmersResponse getWarmersResponse = client.admin().indices().prepareGetWarmers().setLocal(true).get();
            assertThat(getWarmersResponse.warmers().size(), equalTo(1));
            Map.Entry<String,ImmutableList<IndexWarmersMetaData.Entry>> entry = getWarmersResponse.warmers().entrySet().iterator().next();
            assertThat(entry.getKey(), equalTo("test"));
            assertThat(entry.getValue().size(), equalTo(1));
            assertThat(entry.getValue().get(0).name(), equalTo("custom_warmer"));
        }
    }

    @Test
    public void testDeleteWarmerAcknowledgement() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        DeleteWarmerResponse deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer().setIndices("test").setName("custom_warmer").get();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            GetWarmersResponse getWarmersResponse = client.admin().indices().prepareGetWarmers().setLocal(true).get();
            assertThat(getWarmersResponse.warmers().size(), equalTo(0));
        }
    }

    @Test
    public void testDeleteMappingAcknowledgement() {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string").get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1");

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").addTypes("type1").get();
        assertThat(getMappingsResponse.mappings().get("test").get("type1"), notNullValue());

        DeleteMappingResponse deleteMappingResponse = client().admin().indices().prepareDeleteMapping("test").setType("type1").get();
        assertThat(deleteMappingResponse.isAcknowledged(), equalTo(true));

        for (Client client : clients()) {
            getMappingsResponse = client.admin().indices().prepareGetMappings("test").addTypes("type1").get();
            assertThat(getMappingsResponse.mappings().size(), equalTo(0));
        }
    }
}
