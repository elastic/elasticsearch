/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.List;

@TimeoutSuite(millis = 200 * TimeUnits.MINUTE)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = DeleteBulkSearchableSnapshotsIT.NUMBER_OF_NODES)
public class DeleteBulkSearchableSnapshotsIT extends BaseFrozenSearchableSnapshotsIntegTestCase {

    static final int NUMBER_OF_NODES = 1;
    // Configure the cache to be able to hold thousands of regions
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(80);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofGb(3);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (DiscoveryNode.canContainData(otherSettings)) {
            builder.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE);
            builder.put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE);
            builder.put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE);
        }
        builder.put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), 6000);
        return builder.build();
    }

    public void testDeleteManySearchableSnapshotIndices() throws Exception {
        assertEquals(NUMBER_OF_NODES, internalCluster().numDataNodes());

        String indexName = randomIdentifier();
        createIndex(indexName);
        indexRandom(true, indexName, 1_000);

        String repositoryName = randomIdentifier();
        createRepository(repositoryName, "fs");

        String snapshotName = randomIdentifier();
        SnapshotInfo snapshot = createSnapshot(repositoryName, snapshotName, List.of(indexName));

        int numberOfIndices = 5_000;
        logger.info("Mounting {} searchable snapshots", numberOfIndices);
        final String indexPrefix = randomIdentifier() + "_";
        for (int i = 0; i < numberOfIndices; i++) {
            String searchableSnapshotName = indexPrefix + i;
            logger.info("Mounting [{}]", searchableSnapshotName);
            mountSnapshot(
                repositoryName,
                snapshotName,
                indexName,
                searchableSnapshotName,
                Settings.EMPTY,
                MountSearchableSnapshotRequest.Storage.SHARED_CACHE
            );
        }
        ensureGreen();

        logger.info("Warming the shared blob cache");
        for (int i = 0; i < numberOfIndices; i++) {
            SearchResponse searchResponse = client().search(
                new SearchRequest(indexPrefix + i).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ).actionGet();
            searchResponse.decRef();
        }

        // Just to minimise the possibility our results are interfered with by a GC
        System.gc();

        long startTime = System.currentTimeMillis();
        client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest("_all")).actionGet();
        logger.info("Deleting {} indices took {}", numberOfIndices, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
    }

    @Override
    protected int numberOfShards() {
        // a shard on each node
        return NUMBER_OF_NODES;
    }
}
