/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.block.ClusterBlocks.EMPTY_CLUSTER_BLOCK;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class SearchWithIndexBlocksIT extends ESIntegTestCase {

    public void testSearchIndicesWithIndexRefreshBlocks() {
        List<String> indices = createIndices();
        Map<String, Integer> numDocsPerIndex = indexDocuments(indices);
        List<String> unblockedIndices = addIndexRefreshBlockToSomeIndices(indices);

        int expectedHits = 0;
        for (String index : unblockedIndices) {
            expectedHits += numDocsPerIndex.get(index);
        }

        assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), expectedHits);
    }

    public void testOpenPITOnIndicesWithIndexRefreshBlocks() {
        List<String> indices = createIndices();
        Map<String, Integer> numDocsPerIndex = indexDocuments(indices);
        List<String> unblockedIndices = addIndexRefreshBlockToSomeIndices(indices);

        int expectedHits = 0;
        for (String index : unblockedIndices) {
            expectedHits += numDocsPerIndex.get(index);
        }

        BytesReference pitId = null;
        try {
            OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest(indices.toArray(new String[0])).keepAlive(
                TimeValue.timeValueSeconds(10)
            ).allowPartialSearchResults(true);
            pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPITRequest).actionGet().getPointInTimeId();
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueSeconds(10)))
            );
            assertHitCount(client().search(searchRequest), expectedHits);
        } finally {
            if (pitId != null) {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            }
        }
    }

    public void testMultiSearchIndicesWithIndexRefreshBlocks() {
        List<String> indices = createIndices();
        Map<String, Integer> numDocsPerIndex = indexDocuments(indices);
        List<String> unblockedIndices = addIndexRefreshBlockToSomeIndices(indices);

        int expectedHits = 0;
        for (String index : unblockedIndices) {
            expectedHits += numDocsPerIndex.get(index);
        }

        final long expectedHitsL = expectedHits;
        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch().setQuery(QueryBuilders.matchAllQuery()))
                .add(prepareSearch().setQuery(QueryBuilders.termQuery("field", "blah"))),
            response -> {
                assertHitCount(Objects.requireNonNull(response.getResponses()[0].getResponse()), expectedHitsL);
                assertHitCount(Objects.requireNonNull(response.getResponses()[1].getResponse()), 0);
            }
        );
    }

    public void testSearchShardsOnIndicesWithIndexRefreshBlocks() {
        List<String> indices = createIndices();
        indexDocuments(indices);
        List<String> unblockedIndices = addIndexRefreshBlockToSomeIndices(indices);

        var resp = client().execute(
            TransportSearchShardsAction.TYPE,
            new SearchShardsRequest(
                indices.toArray(new String[0]),
                IndicesOptions.DEFAULT,
                new MatchAllQueryBuilder(),
                null,
                null,
                true,
                null
            )
        ).actionGet();
        for (SearchShardsGroup group : resp.getGroups()) {
            assertTrue(unblockedIndices.contains(group.shardId().getIndex().getName()));
        }
    }

    private List<String> createIndices() {
        int numIndices = randomIntBetween(1, 3);
        List<String> indices = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            indices.add("test" + i);
            createIndex("test" + i);
        }
        return indices;
    }

    private Map<String, Integer> indexDocuments(List<String> indices) {
        Map<String, Integer> numDocsPerIndex = new HashMap<>();
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (String index : indices) {
            int numDocs = randomIntBetween(0, 10);
            numDocsPerIndex.put(index, numDocs);
            for (int i = 0; i < numDocs; i++) {
                indexRequests.add(prepareIndex(index).setId(String.valueOf(i)).setSource("field", "value"));
            }
        }
        indexRandom(true, indexRequests);

        return numDocsPerIndex;
    }

    private List<String> addIndexRefreshBlockToSomeIndices(List<String> indices) {
        List<String> unblockedIndices = new ArrayList<>();
        var blocksBuilder = ClusterBlocks.builder().blocks(EMPTY_CLUSTER_BLOCK);
        for (String index : indices) {
            boolean blockIndex = randomBoolean();
            if (blockIndex) {
                blocksBuilder.addIndexBlock(ProjectId.DEFAULT, index, IndexMetadata.INDEX_REFRESH_BLOCK);
            } else {
                unblockedIndices.add(index);
            }
        }

        var dataNodes = clusterService().state().getNodes().getAllNodes();
        for (DiscoveryNode dataNode : dataNodes) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, dataNode.getName());
            ClusterState currentState = clusterService.state();
            ClusterState newState = ClusterState.builder(currentState).blocks(blocksBuilder).build();
            setState(clusterService, newState);
        }

        return unblockedIndices;
    }
}
