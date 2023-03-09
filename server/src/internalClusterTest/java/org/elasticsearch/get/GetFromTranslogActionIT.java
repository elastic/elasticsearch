/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.get;

import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction.Response;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GetFromTranslogActionIT extends ESIntegTestCase {
    public void testGet() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping("field1", "type=keyword,store=true")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1)
                        // A GetFromTranslogAction runs only Stateless where there is only one active indexing shard.
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                )
                .addAlias(new Alias("alias").writeIndex(randomFrom(true, false, null)))
        );
        ensureGreen();

        GetRequest request1 = client().prepareGet(indexOrAlias(), "1").request();
        Response response1 = getFromTranslog(request1);
        assertNotExists(response1);

        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();

        var response2 = getFromTranslog(request1);
        assertExists(response2);

        var indexResponse = client().prepareIndex("test").setSource("field1", "value2").get();
        var request2 = client().prepareGet(indexOrAlias(), indexResponse.getId()).request();
        var response3 = getFromTranslog(request2);
        assertExists(response3);

        client().admin().indices().refresh(new RefreshRequest("test")).get();

        var response4 = getFromTranslog(request1);
        assertNotExists(response4);
        var response5 = getFromTranslog(request2);
        assertNotExists(response5);
    }

    private void assertExists(TransportGetFromTranslogAction.Response response) {
        assertThat(response.multiGetShardResponse().responses().size(), equalTo(1));
        GetResponse getResponse = response.multiGetShardResponse().responses().get(0);
        assertNotNull(getResponse);
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(response.segmentGeneration(), equalTo(-1L));
    }

    private void assertNotExists(TransportGetFromTranslogAction.Response response) {
        assertThat(response.multiGetShardResponse().responses().size(), equalTo(1));
        GetResponse getResponse = response.multiGetShardResponse().responses().get(0);
        assertNull(getResponse);
        assertThat(response.segmentGeneration(), greaterThan(0L));
    }

    private Response getFromTranslog(GetRequest getRequest) throws Exception {
        var shardRouting = randomFrom(clusterService().state().routingTable().allShards("test"));
        var node = clusterService().state().nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        getRequest.setInternalShardId(shardRouting.shardId());
        var multiGetShardRequest = TransportGetAction.asMultiGetShardRequest(getRequest);
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            multiGetShardRequest,
            new ActionListenerResponseHandler<>(response, Response::new, ThreadPool.Names.GET)
        );
        return response.get();
    }

    private String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
