/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shard recovery that reads translog blobs should report what it replayed without logging on every shard initialization.
 */
public class StatelessTranslogRecoveryLoggingIT extends AbstractStatelessPluginIntegTestCase {

    public void testRecoveryReportsTheOperationsItReplayed() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();

        final String indexName = "recovery-logging";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
        flush(indexName);
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());

        internalCluster().stopNode(indexNode);

        try (var mockLog = MockLog.capture(TranslogReplicatorReader.class)) {
            mockLog.addExpectation(
                new MockLog.PatternSeenEventExpectation(
                    "recovery summary",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.INFO,
                    ".*stateless translog recovery.*operationsRead=[1-9][0-9]*.*"
                )
            );
            startIndexNode();
            ensureGreen(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader:DEBUG", reason = "verify empty reader")
    public void testRecoveryWithoutBlobsDoesNotLogInfo() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();

        final String indexName = "recovery-logging-empty";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
        flush(indexName);

        final List<String> translogNodeIds = List.copyOf(getObjectStoreService(indexNode).getNodesWithTranslogBlobContainers());
        internalCluster().stopNode(indexNode);

        // Leave the shard with no translog blobs to read.
        final ObjectStoreService objectStore = getCurrentMasterObjectStoreService();
        for (String ephemeralId : translogNodeIds) {
            final BlobContainer container = objectStore.getTranslogBlobContainer(ephemeralId);
            container.deleteBlobsIgnoringIfNotExists(
                OperationPurpose.TRANSLOG,
                List.copyOf(container.listBlobs(OperationPurpose.TRANSLOG).keySet()).iterator()
            );
        }

        try (var mockLog = MockLog.capture(TranslogReplicatorReader.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reader opened without blobs",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.DEBUG,
                    "*translog replicator reader opened for recovery []*"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "recovery summary without blobs",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.INFO,
                    "*stateless translog recovery*"
                )
            );
            startIndexNode();
            ensureGreen(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }
}
