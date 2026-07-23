/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommitUploadTask;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;

public class UploadProgressLoggingIT extends AbstractStatelessPluginIntegTestCase {

    private static final int UPLOAD_MAX_COMMITS = 2;

    private static final TimeValue HOT_THREADS_LOG_INTERVAL = TimeValue.timeValueMillis(200);

    private static final TimeValue SLOW_UPLOAD_LOG_THRESHOLD = TimeValue.timeValueMillis(100);

    /**
     * Delay applied in the mock object store before each BCC {@code writeBlobAtomic} so uploads stay in flight long enough for
     * observability hooks to fire.
     */
    private static final TimeValue BCC_UPLOAD_DELAY = TimeValue.timeValueMillis(600);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    private static Settings uploadObservabilitySettings(TimeValue hotThreadsLogInterval, TimeValue slowLogThreshold) {
        return Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), UPLOAD_MAX_COMMITS)
            .put(ObjectStoreService.OBJECT_STORE_UPLOAD_HOT_THREADS_LOG_INTERVAL.getKey(), hotThreadsLogInterval)
            .put(StatelessCommitService.STATELESS_UPLOAD_SLOW_LOG_THRESHOLD.getKey(), slowLogThreshold)
            .put(ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS.getKey(), false)
            .build();
    }

    private static StatelessMockRepositoryStrategy delayedBccUploadStrategy() {
        return new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                delayBccUploadIfNeeded(purpose);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }

            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                long blobSize,
                BlobContainer.BlobMultiPartInputStreamProvider provider,
                boolean failIfAlreadyExists
            ) throws IOException {
                delayBccUploadIfNeeded(purpose);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, blobSize, provider, failIfAlreadyExists);
            }

            private void delayBccUploadIfNeeded(OperationPurpose purpose) {
                if (purpose == OperationPurpose.INDICES) {
                    ESTestCase.safeSleep(BCC_UPLOAD_DELAY);
                }
            }
        };
    }

    @TestLogging(
        reason = "need INFO from upload task to observe hot threads logging during upload",
        value = "org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommitUploadTask:INFO"
    )
    public void testBccUploadLogsHotThreadsWhileInFlight() throws Exception {
        startMasterAndIndexNode(
            uploadObservabilitySettings(HOT_THREADS_LOG_INTERVAL, TimeValue.timeValueDays(1)),
            delayedBccUploadStrategy()
        );
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1gb")
                .build()
        );
        ensureGreen(indexName);

        MockLog.awaitLogger(
            () -> triggerBccUpload(indexName),
            BatchedCompoundCommitUploadTask.class,
            new MockLog.SeenEventExpectation(
                "hot threads during upload",
                BatchedCompoundCommitUploadTask.class.getCanonicalName(),
                Level.INFO,
                "*bcc upload*"
            )
        );
    }

    @TestLogging(
        reason = "need INFO from commit service to observe slow upload timing logs",
        value = "org.elasticsearch.xpack.stateless.commits.StatelessCommitService:INFO"
    )
    public void testBccUploadLogsSlowUploadTimings() throws Exception {
        var indexNode = startMasterAndIndexNode(
            uploadObservabilitySettings(TimeValue.ZERO, SLOW_UPLOAD_LOG_THRESHOLD),
            delayedBccUploadStrategy()
        );
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1gb")
                .build()
        );
        ensureGreen(indexName);

        MockLog.awaitLogger(
            () -> triggerBccUpload(indexName),
            StatelessCommitService.class,
            new MockLog.SeenEventExpectation(
                "slow upload timings",
                StatelessCommitService.class.getCanonicalName(),
                Level.INFO,
                "*upload took*attempt*generationQueue=*objectStoreQueue=*uploadIo=*throughput=*"
            )
        );

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        assertThat(shardCommitsContainer.listBlobs(operationPurpose).size(), greaterThan(0));
    }

    @TestLogging(
        reason = "need INFO from commit service to verify slow upload logging stays disabled",
        value = "org.elasticsearch.xpack.stateless.commits.StatelessCommitService:INFO"
    )
    public void testBccUploadDoesNotLogSlowUploadWhenDisabled() throws Exception {
        startMasterAndIndexNode(uploadObservabilitySettings(TimeValue.ZERO, TimeValue.ZERO), delayedBccUploadStrategy());
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1gb")
                .build()
        );
        ensureGreen(indexName);

        MockLog.assertThatLogger(
            () -> triggerBccUpload(indexName),
            StatelessCommitService.class,
            new MockLog.UnseenEventExpectation(
                "no slow upload log",
                StatelessCommitService.class.getCanonicalName(),
                Level.INFO,
                "*upload took*attempt*"
            )
        );
    }

    private void triggerBccUpload(String indexName) {
        for (int i = 0; i < UPLOAD_MAX_COMMITS; i++) {
            indexDocsForBccUpload(indexName, between(30, 50));
            refresh(indexName);
        }
        assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
    }

    private void indexDocsForBccUpload(String indexName, int numDocs) {
        var bulkRequest = client().prepareBulk(indexName);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("payload", randomAlphaOfLengthBetween(8_000, 12_000)));
        }
        assertNoFailures(bulkRequest.get());
    }
}
