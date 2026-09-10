/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class StatelessUpdateIT extends AbstractStatelessPluginIntegTestCase {
    public void testUpdateDoesNotRequireFlush() {
        testUpdate(false);
    }

    public void testBulkUpdateDoesNotRequireFlush() {
        testUpdate(true);
    }

    private void testUpdate(boolean useBulkApi) {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var bulkRequest = client().prepareBulk();
        int requestsInBulk = randomIntBetween(2, 5);
        for (int i = 0; i < requestsInBulk; i++) {
            var indexRequest = client().prepareIndex(indexName).setSource("field", "value1").setId(String.valueOf(i));
            bulkRequest.add(indexRequest);
        }
        assertNoFailures(bulkRequest.get());

        var commitUploads = new AtomicInteger(0);
        setNodeRepositoryStrategy(indexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                commitUploads.incrementAndGet();
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        var idToUpdate = String.valueOf(randomIntBetween(0, requestsInBulk - 1));

        // At this point, translog locations are not tracked in the live version map since there were no realtime gets.
        // During the update, we'll perform a refresh since there are no translog location to read from
        // but it is not required to do a flush.
        // Flush is undesirable since it can take significant amount of time with write thread blocked
        // and holding a primary operation permit.
        var update = client().prepareUpdate(indexName, idToUpdate).setDoc(Map.of("field", "value2"));
        if (useBulkApi) {
            var bulkWithUpdate = client().prepareBulk().add(update).execute().actionGet();
            assertNoFailures(bulkWithUpdate);
        } else {
            update.execute().actionGet();
        }

        // This is a trick to catch any possible commit uploads that are in flight (if this test is about to fail).
        // We should only see one upload that is produced by the flush below.
        flush(indexName);
        assertEquals(1, commitUploads.get());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofBytes(1))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }
}
