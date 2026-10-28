/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cluster.coordination;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.blobstore.OperationPurpose.CLUSTER_STATE;

public class BlobStoreSyncDirectoryIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testNodeShutdownWithPendingClusterStateFileDeletions() throws Exception {
        final var nodeName = startMasterAndIndexNode();

        // Generate cluster state commits that accumulate stale files in BlobStoreSyncDirectory
        for (int i = 0; i < 5; i++) {
            final var indexName = "index-" + i;
            createIndex(indexName, 1, 0);
            indexDocs(indexName, between(10, 50));
            indicesAdmin().prepareFlush(indexName).setForce(true).get();
        }

        final var objectStore = ObjectStoreTestUtils.getObjectStoreStatelessMockRepository(getObjectStoreService(nodeName));

        // Set up a strategy that blocks the first CLUSTER_STATE deletion so we can coordinate the timing between a pending deletion
        // and blob store closure during shutdown. After the block is released (post blob store closure), the strategy checks the
        // object store's lifecycle state and throws AlreadyClosedException if closed.
        // This behavior is consistent with GoogleCloudStorageBlobStore (GCS). S3 and Azure blob stores are closed when their
        // corresponding plugins are closed. Fundamentally, they are still similar to GCS since both all three blob stores are closed
        // as part of Node.pluginLifecycleComponents which is before GatewayMetaState.
        final var deleteAttemptedLatch = new CountDownLatch(1);
        final var proceedWithDeleteLatch = new CountDownLatch(1);
        final var firstDelete = new AtomicBoolean(true);
        setNodeRepositoryStrategy(nodeName, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                if (purpose == CLUSTER_STATE && firstDelete.compareAndSet(true, false)) {
                    deleteAttemptedLatch.countDown();
                    safeAwait(proceedWithDeleteLatch);
                }
                if (purpose == CLUSTER_STATE && objectStore.lifecycleState() == Lifecycle.State.CLOSED) {
                    throw new AlreadyClosedException("blob store is closed");
                }
                originalRunnable.run();
            }
        });

        // Trigger more cluster state changes so BlobStoreSyncDirectory schedules stale file deletions
        for (int i = 5; i < 10; i++) {
            createIndex("index-" + i, 1, 0);
        }

        // Wait for a delete task to be blocked in the strategy
        safeAwait(deleteAttemptedLatch);

        final var plugin = findPlugin(nodeName, TestStatelessPlugin.class);

        // Stop the node in a background thread. During shutdown, ObjectStoreService closes the
        // blob store (setting the closed flag) before StatelessPlugin.close() is called.
        final var stopThread = new Thread(() -> {
            try {
                internalCluster().stopNode(nodeName);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        stopThread.start();

        // Wait for StatelessPlugin.close() to be called, which happens after ObjectStoreService has closed the blob store
        safeAwait(plugin.pluginCloseCalled);

        // Release the blocked delete - the original runnable will now check the blob store's
        // closed state and throw AlreadyClosedException, which BlobStoreSyncDirectory handles
        proceedWithDeleteLatch.countDown();

        safeJoin(stopThread);
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        final CountDownLatch pluginCloseCalled = new CountDownLatch(1);

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public void close() throws IOException {
            pluginCloseCalled.countDown();
            super.close();
        }
    }
}
