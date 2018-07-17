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


package org.elasticsearch.action.bulk;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

public class BulkIntegrationIT extends ESIntegTestCase {
    public void testBulkIndexCreatesMapping() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/bulk-log.json");
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        bulkBuilder.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null, XContentType.JSON);
        bulkBuilder.get();
        assertBusy(() -> {
            GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings().get();
            assertTrue(mappingsResponse.getMappings().containsKey("logstash-2014.03.30"));
            assertTrue(mappingsResponse.getMappings().get("logstash-2014.03.30").containsKey("logs"));
        });
    }

    /**
     * This tests that the {@link TransportBulkAction} evaluates alias routing values correctly when dealing with
     * an alias pointing to multiple indices, while a write index exits.
     */
    public void testBulkWithWriteIndexAndRouting() {
        Map<String, Integer> twoShardsSettings = Collections.singletonMap(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2);
        client().admin().indices().prepareCreate("index1")
            .addAlias(new Alias("alias1").indexRouting("0")).setSettings(twoShardsSettings).get();
        client().admin().indices().prepareCreate("index2")
            .addAlias(new Alias("alias1").indexRouting("0").writeIndex(randomFrom(false, null)))
            .setSettings(twoShardsSettings).get();
        client().admin().indices().prepareCreate("index3")
            .addAlias(new Alias("alias1").indexRouting("1").writeIndex(true)).setSettings(twoShardsSettings).get();

        IndexRequest indexRequest = new IndexRequest("index3", "type", "id");
        boolean indexWithRouting = randomBoolean();
        if (indexWithRouting) {
            indexRequest.routing("0");
        }
        indexRequest.source(Collections.singletonMap("foo", "bar"));
        IndexRequest indexRequestWithAlias = new IndexRequest("alias1", "type", "id");
        indexRequestWithAlias.source(Collections.singletonMap("foo", "baz"));
        BulkResponse bulkResponse = client().prepareBulk()
            .add(indexRequest).add(indexRequestWithAlias).get();
        assertThat(bulkResponse.getItems()[0].getResponse().getIndex(), equalTo("index3"));
        assertThat(bulkResponse.getItems()[0].getResponse().getShardId().getId(), equalTo(1));
        assertThat(bulkResponse.getItems()[0].getResponse().getVersion(), equalTo(1L));
        assertThat(bulkResponse.getItems()[0].getResponse().status(), equalTo(RestStatus.CREATED));
        assertThat(bulkResponse.getItems()[1].getResponse().getIndex(), equalTo("index3"));
        assertThat(bulkResponse.getItems()[1].getResponse().getShardId().getId(), equalTo(0));
        assertThat(bulkResponse.getItems()[1].getResponse().getVersion(), equalTo(1L));
        assertThat(bulkResponse.getItems()[1].getResponse().status(), equalTo(RestStatus.CREATED));
        GetRequestBuilder getBuilder = client().prepareGet("index3", "type", "id");
        if (indexWithRouting) {
            getBuilder.setRouting("0");
        }
        assertThat(getBuilder.get().getSource().get("foo"), equalTo("bar"));
        assertThat(client().prepareGet("index3", "type", "id").setRouting("1").get().getSource().get("foo"), equalTo("baz"));

        client().prepareBulk().add(client().prepareUpdate("alias1", "type", "id").setDoc("foo", "updated")).get();
        assertThat(getBuilder.get().getSource().get("foo"), equalTo("bar"));
        assertThat(client().prepareGet("index3", "type", "id").setRouting("1").get().getSource().get("foo"), equalTo("updated"));
        client().prepareBulk().add(client().prepareDelete("alias1", "type", "id")).get();
        assertThat(getBuilder.get().getSource().get("foo"), equalTo("bar"));
        assertFalse(client().prepareGet("index3", "type", "id").setRouting("1").get().isExists());
    }
}
