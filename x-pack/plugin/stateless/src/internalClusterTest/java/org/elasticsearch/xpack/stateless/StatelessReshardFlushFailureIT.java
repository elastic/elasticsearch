/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshThrottler;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class StatelessReshardFlushFailureIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(org.elasticsearch.xpack.stateless.TestStatelessPlugin.class);
        return plugins;
    }

    private static final AtomicInteger flushFailureCountdown = new AtomicInteger(0);

    public void testSourceReleasesPermitsUponFlushFailure() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        Index index = resolveIndex(indexName);
        ensureGreen(indexName);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
        var setFlushFailureCountdown = new AtomicBoolean(false);
        splitSourceService.setPreHandoffHook(() -> {
            if (setFlushFailureCountdown.getAndSet(true) == false) {
                // Fail the second flush which occurs after acquiring permits
                flushFailureCountdown.set(2);
            }
        });

        logger.info("starting reshard");
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        awaitClusterState((state) -> state.getMetadata().indexMetadata(index).getReshardingMetadata() == null);
        ensureGreen(indexName);
        refresh(indexName);
        assertHitCount(
            client().prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10000)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(false),
            numDocs
        );
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshThrottlerFactory,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) {
                    final ShardId shardId = engineConfig.getShardId();
                    // Fail if countdown goes to 0
                    if (flushFailureCountdown.getAndUpdate(val -> val == 0 ? 0 : val - 1) == 1) {
                        if (randomBoolean()) {
                            listener.onFailure(new EngineException(shardId, "test failure"));
                        } else {
                            throw new IllegalArgumentException("test flush exception");
                        }
                    } else {
                        super.flushHoldingLock(force, waitIfOngoing, listener);
                    }
                }
            };
        }
    }
}
