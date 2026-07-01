/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.get;

import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction.Response;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class GetFromTranslogActionIT extends ESIntegTestCase {

    private static final String INDEX = "test";
    private static final String ALIAS = "alias";

    public void testGetFromTranslog() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setMapping("field1", "type=keyword,store=true")
                .setSettings(
                    // A GetFromTranslogAction runs only Stateless where there is only one active indexing shard.
                    indexSettings(1, 0).put("index.refresh_interval", -1)
                )
                .addAlias(new Alias(ALIAS).writeIndex(randomFrom(true, false, null)))
        );
        ensureGreen();

        var shardRouting = randomFrom(clusterService().state().routingTable().allShards(INDEX));
        var indicesService = internalCluster().getInstance(
            IndicesService.class,
            clusterService().state().nodes().get(shardRouting.currentNodeId()).getName()
        );
        var initialGeneration = indicesService.indexServiceSafe(shardRouting.index())
            .getShard(shardRouting.id())
            .getEngineOrNull()
            .getLastCommittedSegmentInfos()
            .getGeneration();

        var response = getFromTranslog(shardRouting, "1");
        assertNull(response.getResult());
        // There hasn't been any switches from unsafe to safe map
        assertThat(response.segmentGeneration(), equalTo(initialGeneration));

        var indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(RefreshPolicy.NONE).get();
        response = getFromTranslog(shardRouting, "1");
        assertNotNull(response.getResult());
        assertThat(response.getResult().isExists(), equalTo(true));
        assertThat(response.getResult().getVersion(), equalTo(indexResponse.getVersion()));
        assertThat(response.segmentGeneration(), equalTo(-1L));
        // Get followed by a delete should still return a result
        client().prepareDelete("test", "1").get();
        response = getFromTranslog(shardRouting, "1");
        assertNotNull("get followed by a delete should still return a result", response.getResult());
        assertThat(response.getResult().isExists(), equalTo(false));
        assertThat(response.segmentGeneration(), equalTo(-1L));

        indexResponse = prepareIndex("test").setSource("field1", "value2").get();
        response = getFromTranslog(shardRouting, indexResponse.getId());
        assertNotNull(response.getResult());
        assertThat(response.getResult().isExists(), equalTo(true));
        assertThat(response.getResult().getVersion(), equalTo(indexResponse.getVersion()));
        assertThat(response.segmentGeneration(), equalTo(-1L));
        // After a refresh we should not be able to get from translog
        refresh("test");
        response = getFromTranslog(shardRouting, indexResponse.getId());
        assertNull("after a refresh we should not be able to get from translog", response.getResult());
        assertThat(response.segmentGeneration(), equalTo(initialGeneration));
        // After two refreshes the LiveVersionMap switches back to append-only and stops tracking IDs
        // Refreshing with empty LiveVersionMap doesn't cause the switch, see {@link LiveVersionMap.Maps#shouldInheritSafeAccess()}.
        prepareIndex("test").setSource("field1", "value3").get();
        refresh("test");
        refresh("test");
        // An optimized index operation marks the maps as unsafe
        prepareIndex("test").setSource("field1", "value4").get();
        response = getFromTranslog(shardRouting, "non-existent");
        assertNull(response.getResult());
        // getFromTranslog in stateful does not trigger flush a new generation.
        // Therefore lastCommittedSegmentInfos does not change which means lastUnsafeSegmentGenerationForGets does not change
        assertThat(response.segmentGeneration(), equalTo(initialGeneration));
    }

    public void testGetFromTranslogOnSliceIndex() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "slice-test";
        assertAcked(
            prepareCreate(index).setMapping("field1", "type=keyword,store=true")
                .setSettings(indexSettings(1, 0).put("index.refresh_interval", -1).put(IndexSettings.SLICE_ENABLED.getKey(), true))
        );
        ensureGreen(index);
        var shardRouting = randomFrom(clusterService().state().routingTable().allShards(index));

        // Warm-up get (as in testGetFromTranslog): the first lookup switches the live version map from append-only to
        // safe access, so subsequent get-from-translog calls can serve freshly indexed docs from the translog.
        assertNull(getFromTranslog(shardRouting, index, "1", "s1").getResult());

        // The same id "1" in two slices must each resolve via their own slice's compound _id term — before routing was
        // threaded through, the shard threw on encodeIdentity(true, id, null). A get from translog triggers a refresh,
        // so (as in testGetFromTranslog) each write is checked by the get immediately following it.
        var ra = client().index(
            new IndexRequest(index).id("1")
                .source("field1", "va")
                .routing("s1")
                .setRoutingFromSlice(true)
                .setRefreshPolicy(RefreshPolicy.NONE)
        ).actionGet();
        var responseA = getFromTranslog(shardRouting, index, "1", "s1");
        assertNotNull(responseA.getResult());
        assertThat(responseA.getResult().isExists(), equalTo(true));
        assertThat(responseA.getResult().getVersion(), equalTo(ra.getVersion()));

        // Deleting (s1, 1) is reflected from the translog for slice s1.
        client().delete(new DeleteRequest(index, "1").routing("s1").setRoutingFromSlice(true)).actionGet();
        responseA = getFromTranslog(shardRouting, index, "1", "s1");
        assertNotNull("get followed by a delete should still return a result", responseA.getResult());
        assertThat(responseA.getResult().isExists(), equalTo(false));

        // The same id in a different slice is an independent document, resolved by its own compound term.
        var rb = client().index(
            new IndexRequest(index).id("1")
                .source("field1", "vb")
                .routing("s2")
                .setRoutingFromSlice(true)
                .setRefreshPolicy(RefreshPolicy.NONE)
        ).actionGet();
        var responseB = getFromTranslog(shardRouting, index, "1", "s2");
        assertNotNull(responseB.getResult());
        assertThat(responseB.getResult().isExists(), equalTo(true));
        assertThat(responseB.getResult().getVersion(), equalTo(rb.getVersion()));
    }

    private Response getFromTranslog(ShardRouting shardRouting, String index, String id, String routing) throws Exception {
        var getRequest = client().prepareGet(index, id).setRouting(routing).request();
        var node = clusterService().state().nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        TransportGetFromTranslogAction.Request request = new TransportGetFromTranslogAction.Request(getRequest, shardRouting.shardId());
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            request,
            new ActionListenerResponseHandler<>(response, Response::new, transportService.getThreadPool().executor(ThreadPool.Names.GET))
        );
        return response.get();
    }

    private Response getFromTranslog(ShardRouting shardRouting, String id) throws Exception {
        var getRequest = client().prepareGet(indexOrAlias(), id).request();
        var node = clusterService().state().nodes().get(shardRouting.currentNodeId());
        assertNotNull(node);
        TransportGetFromTranslogAction.Request request = new TransportGetFromTranslogAction.Request(getRequest, shardRouting.shardId());
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            node,
            TransportGetFromTranslogAction.NAME,
            request,
            new ActionListenerResponseHandler<>(response, Response::new, transportService.getThreadPool().executor(ThreadPool.Names.GET))
        );
        return response.get();
    }

    private String indexOrAlias() {
        return randomBoolean() ? INDEX : ALIAS;
    }
}
