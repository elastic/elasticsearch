/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.slice;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class SliceIndexingPitIT extends ESIntegTestCase {

    public void testPitSearchOnSliceEnabledIndex() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        String index = "slice-pit-source";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 1).put("number_of_replicas", 0)
            ).get()
        );

        client().index(new IndexRequest(index).id("1").routing("tenant-a").setRoutingFromSlice(true).source("value", "a")).get();
        client().index(new IndexRequest(index).id("2").routing("tenant-b").setRoutingFromSlice(true).source("value", "b")).get();
        indicesAdmin().prepareRefresh(index).get();

        BytesReference pitId = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet().getPointInTimeId();

        try {
            assertResponse(
                prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)).setSize(10),
                response -> assertHitCount(response, 2)
            );

            var tenantASearch = prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)).setSize(10);
            tenantASearch.request().searchSlice("tenant-a");
            assertResponse(tenantASearch, response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            });

            var tenantBSearch = prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)).setSize(10);
            tenantBSearch.request().searchSlice("tenant-b");
            assertResponse(tenantBSearch, response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            });
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testOpenPitWithSliceScopesShards() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        String index = "slice-pit-open";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 2).put("number_of_replicas", 0)
            ).get()
        );

        client().index(new IndexRequest(index).id("1").routing("tenant-a").setRoutingFromSlice(true).source("value", "a")).get();
        client().index(new IndexRequest(index).id("2").routing("tenant-b").setRoutingFromSlice(true).source("value", "b")).get();
        indicesAdmin().prepareRefresh(index).get();

        var openSliceA = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).searchSlice("tenant-a").keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet();
        assertThat(openSliceA.getTotalShards(), equalTo(1));
        BytesReference sliceAPitId = openSliceA.getPointInTimeId();

        try {
            var tenantASearch = prepareSearch().setPointInTime(new PointInTimeBuilder(sliceAPitId)).setSize(10);
            tenantASearch.request().searchSlice("tenant-a");
            assertResponse(tenantASearch, response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            });
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(sliceAPitId)).actionGet();
        }

        var openAll = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).searchSlice(SliceIndexing.SLICE_ALL).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet();
        assertThat(openAll.getTotalShards(), equalTo(2));
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(openAll.getPointInTimeId())).actionGet();
    }

    /**
     * Verifies opening a PIT with {@code slice} only creates reader contexts on the shard
     * that contains the slice routing, leaving documents on other shards inaccessible.
     */
    public void testOpenPitWithSliceOnlyAffectsMatchingShard() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        String index = "slice-pit-single-shard";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 2).put("number_of_replicas", 0)
            ).get()
        );

        IndexMetadata indexMetadata = clusterService().state().projectState().metadata().index(index);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        String sliceOnShard0 = routingForShard(indexRouting, 0);
        String sliceOnShard1 = routingForShard(indexRouting, 1);
        assertThat(indexRouting.indexShard(new IndexRequest().id("0").routing(sliceOnShard0)), equalTo(0));
        assertThat(indexRouting.indexShard(new IndexRequest().id("1").routing(sliceOnShard1)), equalTo(1));

        client().index(new IndexRequest(index).id("shard-0-doc").routing(sliceOnShard0).setRoutingFromSlice(true).source("value", "s0"))
            .get();
        client().index(new IndexRequest(index).id("shard-1-doc").routing(sliceOnShard1).setRoutingFromSlice(true).source("value", "s1"))
            .get();
        indicesAdmin().prepareRefresh(index).get();

        OpenPointInTimeResponse openShard0 = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).searchSlice(sliceOnShard0).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet();
        assertThat(openShard0.getTotalShards(), equalTo(1));
        BytesReference shard0PitId = openShard0.getPointInTimeId();
        try {
            SearchContextId searchContextId = SearchContextId.decode(writableRegistry(), shard0PitId);
            Set<Integer> openedShardIds = searchContextId.shards().keySet().stream().map(ShardId::getId).collect(Collectors.toSet());
            assertThat(openedShardIds, hasSize(1));
            assertThat(openedShardIds.iterator().next(), equalTo(0));

            assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(shard0PitId)).setSize(10), response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("shard-0-doc"));
            });
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(shard0PitId)).actionGet();
        }

        OpenPointInTimeResponse openShard1 = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).searchSlice(sliceOnShard1).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet();
        assertThat(openShard1.getTotalShards(), equalTo(1));
        BytesReference shard1PitId = openShard1.getPointInTimeId();
        try {
            SearchContextId searchContextId = SearchContextId.decode(writableRegistry(), shard1PitId);
            Set<Integer> openedShardIds = searchContextId.shards().keySet().stream().map(ShardId::getId).collect(Collectors.toSet());
            assertThat(openedShardIds, hasSize(1));
            assertThat(openedShardIds.iterator().next(), equalTo(1));

            assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(shard1PitId)).setSize(10), response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("shard-1-doc"));
            });
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(shard1PitId)).actionGet();
        }
    }

    private static String routingForShard(IndexRouting indexRouting, int targetShard) {
        for (int i = 0; i < 1000; i++) {
            String routing = "slice-" + i;
            if (indexRouting.indexShard(new IndexRequest().id("probe").routing(routing)) == targetShard) {
                return routing;
            }
        }
        throw new AssertionError("failed to find routing for shard [" + targetShard + "]");
    }
}
