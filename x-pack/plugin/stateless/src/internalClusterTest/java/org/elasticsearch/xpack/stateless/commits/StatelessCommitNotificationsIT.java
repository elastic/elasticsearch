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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StatelessCommitNotificationsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    public static class TestStateless extends Stateless {
        public final AtomicReference<CyclicBarrier> afterFlushBarrierRef = new AtomicReference<>();

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                client,
                commitCleaner,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider,
            TranslogRecoveryMetrics translogRecoveryMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                sharedBlobCacheWarmingService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                translogRecoveryMetrics
            ) {
                @Override
                protected void afterFlush(long generation) {
                    final CyclicBarrier barrier = afterFlushBarrierRef.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    super.afterFlush(generation);
                }
            };
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {

        public AtomicReference<CyclicBarrier> getMaxUploadedBccTermAndGenBarrierRef = new AtomicReference<>();

        public TestStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService, telemetryProvider);
        }

        @Override
        protected ShardCommitState createShardCommitState(
            ShardId shardId,
            long primaryTerm,
            BooleanSupplier inititalizingNoSearchSupplier
        ) {
            return new ShardCommitState(shardId, primaryTerm, inititalizingNoSearchSupplier) {
                @Override
                public PrimaryTermAndGeneration getMaxUploadedBccTermAndGen() {
                    final CyclicBarrier barrier = getMaxUploadedBccTermAndGenBarrierRef.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    return super.getMaxUploadedBccTermAndGen();
                }
            };
        }
    }

    public void testAlwaysSendCommitNotificationOnCreation() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
                .build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(10, 20));
        refresh(indexName);

        final TestStateless testStateless = findPlugin(indexNode, TestStateless.class);
        final CyclicBarrier afterFlushBarrier = new CyclicBarrier(2);
        testStateless.afterFlushBarrierRef.set(afterFlushBarrier);

        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final TestStatelessCommitService commitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final CyclicBarrier getMaxUploadedBccTermAndGenBarrier = new CyclicBarrier(2);
        commitService.getMaxUploadedBccTermAndGenBarrierRef.set(getMaxUploadedBccTermAndGenBarrier);

        final List<NewCommitNotificationRequest> requests = Collections.synchronizedList(new ArrayList<>());
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                requests.add((NewCommitNotificationRequest) request);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final Thread flushThread = new Thread(() -> flush(indexName));
        flushThread.start();
        // Wait for the flush thread to enter afterFlush so that it is about to call ensureMaxGenerationToUploadForFlush
        // which uploads the current VBCC
        safeAwait(afterFlushBarrier);
        testStateless.afterFlushBarrierRef.set(null);

        indexDocs(indexName, between(10, 20));
        final Thread refreshThread = new Thread(() -> refresh(indexName));
        refreshThread.start();
        // Wait for the refresh thread to append the new commit and checking the maxUploadedBccTermAndGen for sending
        // out new commit notification on creation
        safeAwait(getMaxUploadedBccTermAndGenBarrier);
        commitService.getMaxUploadedBccTermAndGenBarrierRef.set(null);

        // Let the flush thread proceed to upload the VBCC
        safeAwait(afterFlushBarrier);
        // Wait till the commit notification to be sent due to the flush which is strictly after uploading the commit
        assertBusy(() -> assertFalse(requests.isEmpty()));
        // Let the refresh thread continue
        safeAwait(getMaxUploadedBccTermAndGenBarrier);

        flushThread.join();
        refreshThread.join();

        // Both notifications (on creation and on upload) are sent out
        assertBusy(() -> assertThat(requests, hasSize(2)));
        // The two notifications are identical because they are for the same VBCC.
        // The notification on creation is also an uploaded notification since the maxUploadedBccTermAndGen gets bumped
        // by the concurrent flush.
        assertTrue("request " + requests.get(0), requests.get(0).isUploaded());
        assertThat(
            "request " + requests.get(0),
            requests.get(0).getCompoundCommit().generation(),
            equalTo(indexEngine.getCurrentGeneration())
        );
        assertThat(
            "request " + requests.get(0),
            requests.get(0).getBatchedCompoundCommitGeneration(),
            equalTo(indexEngine.getCurrentGeneration() - 1L)
        );
        assertThat(requests.get(0), equalTo(requests.get(1)));

        ensureGreen(indexName);
    }
}
