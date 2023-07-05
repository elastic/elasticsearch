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
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction.Response;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GetFromTranslogActionIT extends ESIntegTestCase {
    public void testGetFromTranslog() throws Exception {
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

        var response = getFromTranslog(indexOrAlias(), "1");
        assertNull(response.getResult());
        // There hasn't been any switches from unsafe to safe map
        assertThat(response.segmentGeneration(), equalTo(-1L));

        var indexResponse = client().prepareIndex("test")
            .setId("1")
            .setSource("field1", "value1")
            .setRefreshPolicy(RefreshPolicy.NONE)
            .get();
        response = getFromTranslog(indexOrAlias(), "1");
        assertNotNull(response.getResult());
        assertThat(response.getResult().isExists(), equalTo(true));
        assertThat(response.getResult().getVersion(), equalTo(indexResponse.getVersion()));
        assertThat(response.segmentGeneration(), equalTo(-1L));
        // Get followed by a delete should still return a result
        client().prepareDelete("test", "1").get();
        response = getFromTranslog(indexOrAlias(), "1");
        assertNotNull("get followed by a delete should still return a result", response.getResult());
        assertThat(response.getResult().isExists(), equalTo(false));
        assertThat(response.segmentGeneration(), equalTo(-1L));

        indexResponse = client().prepareIndex("test").setSource("field1", "value2").get();
        response = getFromTranslog(indexOrAlias(), indexResponse.getId());
        assertNotNull(response.getResult());
        assertThat(response.getResult().isExists(), equalTo(true));
        assertThat(response.getResult().getVersion(), equalTo(indexResponse.getVersion()));
        assertThat(response.segmentGeneration(), equalTo(-1L));
        // After a refresh we should not be able to get from translog
        refresh("test");
        response = getFromTranslog(indexOrAlias(), indexResponse.getId());
        assertNull("after a refresh we should not be able to get from translog", response.getResult());
        assertThat(response.segmentGeneration(), equalTo(-1L));
        // After two refreshes the LiveVersionMap switches back to append-only and stops tracking IDs
        // Refreshing with empty LiveVersionMap doesn't cause the switch, see {@link LiveVersionMap.Maps#shouldInheritSafeAccess()}.
        client().prepareIndex("test").setSource("field1", "value3").get();
        refresh("test");
        refresh("test");
        // An optimized index operation marks the maps as unsafe
        client().prepareIndex("test").setSource("field1", "value4").get();
        response = getFromTranslog(indexOrAlias(), "non-existent");
        assertNull(response.getResult());
        assertThat(response.segmentGeneration(), greaterThan(0L));
    }

    private Response getFromTranslog(String index, String id) throws Exception {
        var getRequest = client().prepareGet(index, id).request();
        var shardRouting = randomFrom(clusterService().state().routingTable().allShards("test"));
        var node = clusterService().state().nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        TransportGetFromTranslogAction.Request request = new TransportGetFromTranslogAction.Request(getRequest, shardRouting.shardId());
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            request,
            new ActionListenerResponseHandler<>(response, Response::new, ThreadPool.Names.GET)
        );
        return response.get();
    }

    private String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
