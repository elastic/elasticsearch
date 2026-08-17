/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.RecoveryRole;
import org.elasticsearch.indices.recovery.RecoverySchedulingListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;

public class StatelessPreFlushRelocationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testRelocationFirstFlushWaitsForOngoingFlushes() throws Exception {
        var sourceNode = startMasterAndIndexNode();
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        var bccUploadBlocked = new CountDownLatch(1);
        var unblockBCCUpload = new CountDownLatch(1);
        // We expect the initial flush upload and the recovery second flush (with acquired permits)
        var expectedBCCUploadsTriggered = new CountDownLatch(2);
        setNodeRepositoryStrategy(sourceNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX)) {
                    bccUploadBlocked.countDown();
                    expectedBCCUploadsTriggered.countDown();
                    safeAwait(unblockBCCUpload);
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        indexDocs(indexName, randomIntBetween(10, 20));
        var flushFuture = indicesAdmin().prepareFlush(indexName).execute();
        assertThat(flushFuture.isDone(), is(false));
        safeAwait(bccUploadBlocked);

        var preRecoveryFlushDone = new PlainActionFuture<Void>();
        var recoveryStarted = new CountDownLatch(1);
        var recoverySchedulingListener = internalCluster().getInstance(CompositeRecoverySchedulingListener.class, sourceNode);
        recoverySchedulingListener.addListener(new RecoverySchedulingListener() {

            @Override
            public void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role) {
                if (type == RecoverySource.Type.PEER && role == RecoveryRole.SOURCE) {
                    recoveryStarted.countDown();
                }
            }

            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                // This event is triggered after the pre-flush completes
                if (type == RecoverySource.Type.PEER && role == RecoveryRole.SOURCE) {
                    preRecoveryFlushDone.onResponse(null);
                }
            }
        });

        startIndexNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);

        safeAwait(recoveryStarted);
        assertThat(preRecoveryFlushDone.isDone(), is(false));

        // ensure that we can continue indexing and that the pre-flush waits for the ongoing flushes
        indexDocs(indexName, randomIntBetween(10, 20));

        // Still waiting for the first commit to be uploaded
        assertThat(preRecoveryFlushDone.isDone(), is(false));

        unblockBCCUpload.countDown();

        // After this point the relocation will acquire all the permits and do a second flush
        safeGet(preRecoveryFlushDone);
        safeAwait(expectedBCCUploadsTriggered);

        ensureGreen(indexName);
    }
}
