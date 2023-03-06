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

        GetRequest getRequest1 = client().prepareGet(indexOrAlias(), "1").request();
        Response getResponse1 = getFromTranslog(getRequest1);
        assertNull(getResponse1.getResult());
        assertThat(getResponse1.segmentGeneration(), greaterThan(0L));

        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();

        var getResponse2 = getFromTranslog(getRequest1);
        assertNotNull(getResponse2.getResult());
        assertThat(getResponse2.getResult().isExists(), equalTo(true));
        assertThat(getResponse2.segmentGeneration(), equalTo(-1L));

        var indexResponse = client().prepareIndex("test").setSource("field1", "value2").get();
        var getRequest2 = client().prepareGet(indexOrAlias(), indexResponse.getId()).request();
        var getResponse3 = getFromTranslog(getRequest2);
        assertNotNull(getResponse3.getResult());
        assertThat(getResponse3.getResult().isExists(), equalTo(true));
        assertThat(getResponse3.segmentGeneration(), equalTo(-1L));

        client().admin().indices().refresh(new RefreshRequest("test")).get();

        var getResponse4 = getFromTranslog(getRequest1);
        assertNull(getResponse4.getResult());
        assertThat(getResponse4.segmentGeneration(), greaterThan(0L));
        var getResponse5 = getFromTranslog(getRequest2);
        assertNull(getResponse5.getResult());
        assertThat(getResponse5.segmentGeneration(), greaterThan(0L));
    }

    private Response getFromTranslog(GetRequest getRequest) throws Exception {
        var shardRouting = randomFrom(clusterService().state().routingTable().allShards("test"));
        var node = clusterService().state().nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        getRequest.setInternalShardId(shardRouting.shardId());
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            getRequest,
            new ActionListenerResponseHandler<>(response, Response::new, ThreadPool.Names.GET)
        );
        return response.get();
    }

    private String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
