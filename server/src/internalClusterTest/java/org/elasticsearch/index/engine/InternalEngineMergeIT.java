/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0, scope = Scope.TEST)
public class InternalEngineMergeIT extends ESIntegTestCase {

    private boolean useThreadPoolMerging;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        useThreadPoolMerging = randomBoolean();
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), useThreadPoolMerging);
        return settings.build();
    }

    public void testMergesHappening() throws Exception {
        final int numOfShards = randomIntBetween(1, 5);
        // some settings to keep num segments low
        assertAcked(prepareCreate("test").setSettings(indexSettings(numOfShards, 0).build()));
        long id = 0;
        final int rounds = scaledRandomIntBetween(50, 300);
        logger.info("Starting rounds [{}] ", rounds);
        for (int i = 0; i < rounds; ++i) {
            final int numDocs = scaledRandomIntBetween(100, 1000);
            BulkRequestBuilder request = client().prepareBulk();
            for (int j = 0; j < numDocs; ++j) {
                request.add(
                    new IndexRequest("test").id(Long.toString(id++))
                        .source(jsonBuilder().startObject().field("l", randomLong()).endObject())
                );
            }
            BulkResponse response = request.get();
            refresh();
            assertNoFailures(response);
            IndicesStatsResponse stats = indicesAdmin().prepareStats("test").setSegments(true).setMerge(true).get();
            logger.info(
                "index round [{}] - segments {}, total merges {}, current merge {}",
                i,
                stats.getPrimaries().getSegments().getCount(),
                stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent()
            );
        }
        final long upperNumberSegments = 2 * numOfShards * 10;

        assertBusy(() -> {
            IndicesStatsResponse stats = indicesAdmin().prepareStats().setSegments(true).setMerge(true).get();
            logger.info(
                "numshards {}, segments {}, total merges {}, current merge {}",
                numOfShards,
                stats.getPrimaries().getSegments().getCount(),
                stats.getPrimaries().getMerge().getTotal(),
                stats.getPrimaries().getMerge().getCurrent()
            );
            long current = stats.getPrimaries().getMerge().getCurrent();
            long count = stats.getPrimaries().getSegments().getCount();
            assertThat(count, lessThan(upperNumberSegments));
            assertThat(current, equalTo(0L));
        });

        IndicesStatsResponse stats = indicesAdmin().prepareStats().setSegments(true).setMerge(true).get();
        logger.info(
            "numshards {}, segments {}, total merges {}, current merge {}",
            numOfShards,
            stats.getPrimaries().getSegments().getCount(),
            stats.getPrimaries().getMerge().getTotal(),
            stats.getPrimaries().getMerge().getCurrent()
        );
        long count = stats.getPrimaries().getSegments().getCount();
        assertThat(count, lessThanOrEqualTo(upperNumberSegments));
    }

    public void testMergesUseTheMergeThreadPool() throws Exception {
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(randomIntBetween(1, 3), 0).build());
        long id = 0;
        final int minMerges = randomIntBetween(1, 5);
        long totalDocs = 0;

        while (true) {
            int docs = randomIntBetween(100, 200);
            totalDocs += docs;

            BulkRequestBuilder request = client().prepareBulk();
            for (int j = 0; j < docs; ++j) {
                request.add(
                    new IndexRequest(indexName).id(Long.toString(id++))
                        .source(jsonBuilder().startObject().field("l", randomLong()).endObject())
                );
            }
            BulkResponse response = request.get();
            assertNoFailures(response);
            refresh(indexName);

            var mergesResponse = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
            var primaries = mergesResponse.getIndices().get(indexName).getPrimaries();
            if (primaries.merge.getTotal() >= minMerges) {
                break;
            }
        }

        forceMerge();
        refresh(indexName);

        // after a force merge there should only be 1 segment per shard
        var shardsWithMultipleSegments = getShardSegments().stream()
            .filter(shardSegments -> shardSegments.getSegments().size() > 1)
            .toList();
        assertTrue("there are shards with multiple segments " + shardsWithMultipleSegments, shardsWithMultipleSegments.isEmpty());

        final long expectedTotalDocs = totalDocs;
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), expectedTotalDocs);

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        long mergeCount = indicesStats.getIndices().get(indexName).getPrimaries().merge.getTotal();
        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setThreadPool(true).get();
        assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));

        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        if (useThreadPoolMerging) {
            assertThat(
                nodeStats.getThreadPool().stats().stream().filter(s -> ThreadPool.Names.MERGE.equals(s.name())).findAny().get().completed(),
                equalTo(mergeCount)
            );
        } else {
            assertTrue(nodeStats.getThreadPool().stats().stream().filter(s -> ThreadPool.Names.MERGE.equals(s.name())).findAny().isEmpty());
        }
    }
}
