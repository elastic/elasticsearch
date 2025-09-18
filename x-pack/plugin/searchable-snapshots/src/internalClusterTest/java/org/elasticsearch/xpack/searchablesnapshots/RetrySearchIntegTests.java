/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class RetrySearchIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockSearchService.TestPlugin.class);
        return plugins;
    }

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
            indexRequestBuilders.add(prepareIndex(indexName).setSource("created_date", "2011-02-02"));
        }
        indexRandom(true, false, indexRequestBuilders);
        assertThat(
            indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        // force merge with expunge deletes is not merging down to one segment only
        forceMerge(false);

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
                ensureGreen(indexName);
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
        int numberOfShards = between(1, 5);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards).build())
                .setMapping("""
                    {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}""")
        );
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final int docCount = between(0, 100);
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(prepareIndex(indexName).setSource("created_date", "2011-02-02"));
        }
        indexRandom(true, false, indexRequestBuilders);
        assertThat(
            indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        // force merge with expunge deletes is not merging down to one segment only
        forceMerge(false);

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
        final BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openRequest).actionGet().getPointInTimeId();
        assertEquals(numberOfShards, SearchContextId.decode(writableRegistry(), pitId).shards().size());
        logger.info(
            "---> PIT id: " + new PointInTimeBuilder(pitId).getSearchContextId(this.writableRegistry()).toString().replace("},", "\n")
        );
        SetOnce<BytesReference> updatedPit = new SetOnce<>();
        try {
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertThat(resp.pointInTimeId(), equalTo(pitId));
                assertHitCount(resp, docCount);
            });
            final Set<String> allocatedNodes = internalCluster().nodesInclude(indexName);
            for (String allocatedNode : allocatedNodes) {
                internalCluster().restartNode(allocatedNode);
                ensureGreen(indexName);
            }
            ensureGreen(indexName);

            assertNoFailuresAndResponse(
                prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
                    // // set preFilterShardSize high enough, we want to hit each shard in this first run to re-create all contexts
                    // .setPreFilterShardSize(128)
                    .setAllowPartialSearchResults(randomBoolean())  // partial results should not matter here
                    .setPointInTime(new PointInTimeBuilder(pitId)),
                resp -> {
                    assertHitCount(resp, docCount);
                    updatedPit.set(resp.pointInTimeId());
                }
            );
            logger.info("--> ran first search after node restart");

            // at this point we should have re-created all contexts, so no need to create them again
            // running the search a second time should not re-trigger creation of new contexts
            final AtomicLong newContexts = new AtomicLong(0);
            for (String allocatedNode : allocatedNodes) {
                MockSearchService searchService = (MockSearchService) internalCluster().getInstance(SearchService.class, allocatedNode);
                searchService.setOnPutContext(context -> { newContexts.incrementAndGet(); });
            }

            assertNoFailuresAndResponse(
                prepareSearch().setQuery(new RangeQueryBuilder("created_date").gte("2011-01-01").lte("2011-12-12"))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreFilterShardSize(between(1, 10))
                    .setAllowPartialSearchResults(randomBoolean())  // partial results should not matter here
                    .setPointInTime(new PointInTimeBuilder(updatedPit.get())),
                resp -> {
                    assertEquals(numberOfShards, resp.getSuccessfulShards());
                    assertHitCount(resp, docCount);
                }
            );
            logger.info("--> ran second search after node restart");
            assertThat("Search should not create new contexts", newContexts.get(), equalTo(0L));
        } catch (Exception e) {
            logger.error("---> unexpected exception", e);
            throw e;
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(updatedPit.get())).actionGet();
        }
    }
}
