/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class RetrySearchIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testSearcherId() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = between(1, 5);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards).build())
                .setMapping("""
                    {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}""")
        );
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final int docCount = between(0, 100);
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("created_date", "2011-02-02"));
        }
        indexRandom(true, false, indexRequestBuilders);
        assertThat(
            indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        forceMerge();

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "fs");

        final SnapshotId snapshotOne = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final int numberOfReplicas = between(0, 2);
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + 1);
        mountSnapshot(repositoryName, snapshotOne.getName(), indexName, indexName, indexSettings);
        ensureGreen(indexName);

        final String[] searcherIds = new String[numberOfShards];
        Set<String> allocatedNodes = internalCluster().nodesInclude(indexName);
        for (String node : allocatedNodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName));
            for (IndexShard indexShard : indexService) {
                try (Engine.SearcherSupplier searcher = indexShard.acquireSearcherSupplier()) {
                    assertNotNull(searcher.getSearcherId());
                    if (searcherIds[indexShard.shardId().id()] != null) {
                        assertThat(searcher.getSearcherId(), equalTo(searcherIds[indexShard.shardId().id()]));
                    } else {
                        searcherIds[indexShard.shardId().id()] = searcher.getSearcherId();
                    }
                }
            }
        }

        for (String allocatedNode : allocatedNodes) {
            if (randomBoolean()) {
                internalCluster().restartNode(allocatedNode);
            }
        }
        ensureGreen(indexName);
        allocatedNodes = internalCluster().nodesInclude(indexName);
        for (String node : allocatedNodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName));
            for (IndexShard indexShard : indexService) {
                try (Engine.SearcherSupplier searcher = indexShard.acquireSearcherSupplier()) {
                    assertNotNull(searcher.getSearcherId());
                    assertThat(searcher.getSearcherId(), equalTo(searcherIds[indexShard.shardId().id()]));
                }
            }
        }
    }

    public void testRetryPointInTime() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build())
                .setMapping("""
                    {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}""")
        );
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final int docCount = between(0, 100);
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("created_date", "2011-02-02"));
        }
        indexRandom(true, false, indexRequestBuilders);
        assertThat(
            indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        forceMerge();

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "fs");

        final SnapshotId snapshotOne = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final int numberOfReplicas = between(0, 2);
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + 1);

        mountSnapshot(repositoryName, snapshotOne.getName(), indexName, indexName, indexSettings);
        ensureGreen(indexName);

        final OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(indexName).indicesOptions(
            IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED
        ).keepAlive(TimeValue.timeValueMinutes(2));
        final String pitId = client().execute(OpenPointInTimeAction.INSTANCE, openRequest).actionGet().getPointInTimeId();
        try {
            SearchResponse resp = client().prepareSearch()
                .setIndices(indexName)
                .setPreference(null)
                .setPointInTime(new PointInTimeBuilder(pitId))
                .get();
            assertNoFailures(resp);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            assertHitCount(resp, docCount);

            final Set<String> allocatedNodes = internalCluster().nodesInclude(indexName);
            for (String allocatedNode : allocatedNodes) {
                internalCluster().restartNode(allocatedNode);
            }
            ensureGreen(indexName);
            resp = client().prepareSearch()
                .setIndices(indexName)
                .setQuery(new RangeQueryBuilder("created_date").gte("2011-01-01").lte("2011-12-12"))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreference(null)
                .setPreFilterShardSize(between(1, 10))
                .setAllowPartialSearchResults(true)
                .setPointInTime(new PointInTimeBuilder(pitId))
                .get();
            assertNoFailures(resp);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            assertHitCount(resp, docCount);
        } finally {
            client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }
}
