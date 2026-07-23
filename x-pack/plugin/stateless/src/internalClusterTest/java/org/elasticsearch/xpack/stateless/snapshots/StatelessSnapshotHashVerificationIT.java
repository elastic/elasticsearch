/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class StatelessSnapshotHashVerificationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        // the object store is backed by StatelessMockRepositoryPlugin instead, which registers the same repository type
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(SnapshotFileEnumerationInterceptPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // use the mock object store so that the test can inject read failures
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // disable hollow shards so that the relocated shard keeps a regular IndexEngine and the snapshot code path stays simple
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false);
    }

    /**
     * Reproduces the snapshot hang behind https://github.com/elastic/elasticsearch/issues/154655: during shard snapshotting,
     * {@code BlobStoreRepository#doSnapshotShard} re-reads "virtual" files (files whose contents are stored in the shard-level
     * metadata, i.e. {@code segments_N} and {@code .si} files) via
     * {@code LocalPrimarySnapshotShardContext#assertFileContentsMatchHash} to verify their hash. On a stateless index node that
     * bootstrapped the shard from the object store, that read goes through the shared blob cache and, on a cache miss, to the
     * object store. A transient read failure there used to be rethrown as an {@link AssertionError}, which escaped the snapshot
     * thread pool worker without ever completing the shard snapshot, leaving the snapshot in progress forever.
     *
     * The test recreates the failure deterministically: it relocates the shard (so its commit files are not on local disk),
     * then uses an intercepted {@link IndexCommit#getFileNames()} — invoked by {@code doSnapshotShard} after loading the store
     * metadata and right before the file enumeration loop — to evict the blob cache and start injecting object store read
     * failures. The next read is the hash-verification read, which is then guaranteed to hit the object store and fail. The
     * injected failures may legitimately fail the shard snapshot, but the create-snapshot call must always complete.
     */
    public void testTransientReadFailureDuringSnapshotFileVerification() throws Exception {
        startMasterOnlyNode();
        final var nodeA = startIndexNode();
        final var nodeB = startIndexNode();

        final var repoName = "test-repo";
        createRepository(repoName, "fs");

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0)
                // a merge after the relocation could rewrite the shard's files locally on the new node, and the test needs the
                // snapshotted commit files to be readable only through the blob cache / object store
                .put(InternalSettingsPlugin.MERGE_ENABLED.getKey(), false)
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeA)
                .build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(32, 128));
        flush(indexName);

        // Relocate the shard so that its commit files on the new node can only be read through the blob cache / object store
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeB), indexName);
        ensureGreen(indexName);

        final var indexShard = findIndexShard(resolveIndex(indexName), 0);
        assertThat(getNodeName(indexShard.routingEntry().currentNodeId()), equalTo(nodeB));
        final var cacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(
            IndexDirectory.unwrapDirectory(indexShard.store().directory()).getBlobStoreCacheDirectory()
        );

        final var readFailuresInjected = new AtomicBoolean();
        findPlugin(nodeB, SnapshotFileEnumerationInterceptPlugin.class).beforeSnapshotFileEnumeration.put(indexShard.shardId(), () -> {
            if (readFailuresInjected.compareAndSet(false, true)) {
                cacheService.forceEvict(cacheKey -> true);
                setNodeRepositoryFailureStrategy(nodeB, true, false, Map.of(OperationPurpose.INDICES, ".*"));
            }
        });

        try {
            final var future = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "test-snap")
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute();
            final CreateSnapshotResponse response;
            try {
                response = future.get(60, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                    "create-snapshot did not complete, the shard snapshot likely failed without reporting its status",
                    e
                );
            }
            assertTrue("the test hook never ran, the snapshot never read the shard's file names", readFailuresInjected.get());
            // The injected object store failures may legitimately fail the shard snapshot, but the snapshot must complete
            assertThat(response.getSnapshotInfo().state(), oneOf(SnapshotState.SUCCESS, SnapshotState.PARTIAL));
        } finally {
            setNodeRepositoryStrategy(nodeB, StatelessMockRepositoryStrategy.DEFAULT);
        }
    }

    private static String getNodeName(String id) {
        return internalCluster().getInstance(ClusterService.class).state().nodes().get(id).getName();
    }

    /**
     * Wraps the {@link IndexCommit} acquired for snapshotting so that a per-shard hook runs when the snapshot code calls
     * {@link IndexCommit#getFileNames()}. In {@code BlobStoreRepository#doSnapshotShard} this happens after the store metadata
     * has been loaded and immediately before the file enumeration loop that verifies virtual file hashes, which is the exact
     * window this test needs to instrument.
     */
    public static class SnapshotFileEnumerationInterceptPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        final Map<ShardId, Runnable> beforeSnapshotFileEnumeration = new ConcurrentHashMap<>();

        public SnapshotFileEnumerationInterceptPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return super.getEngineFactory(indexSettings).map(factory -> engineConfig -> {
                final var delegate = engineConfig.getSnapshotCommitSupplier();
                final var shardId = engineConfig.getShardId();
                final IndexStorePlugin.SnapshotCommitSupplier wrappedCommitSupplier = engine -> {
                    final var commitRef = delegate.acquireIndexCommitForSnapshot(engine);
                    final var wrappedCommit = new FilterIndexCommit(commitRef.getIndexCommit()) {
                        @Override
                        public Collection<String> getFileNames() throws IOException {
                            final var hook = beforeSnapshotFileEnumeration.get(shardId);
                            if (hook != null) {
                                hook.run();
                            }
                            return super.getFileNames();
                        }
                    };
                    return new Engine.IndexCommitRef(wrappedCommit, commitRef::close);
                };
                final var wrappedConfig = EngineConfig.builder(engineConfig).snapshotCommitSupplier(wrappedCommitSupplier).build();
                return factory.newReadWriteEngine(wrappedConfig);
            });
        }
    }
}
