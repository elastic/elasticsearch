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
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shard recovery should say what it replayed, whether or not it replayed anything. A recovery that silently replays
 * nothing is indistinguishable from one that had nothing to replay, which makes it impossible to tell after the fact
 * whether acknowledged operations were lost.
 */
public class StatelessTranslogRecoveryLoggingIT extends AbstractStatelessPluginIntegTestCase {

    private static final String RECOVERY_MESSAGE = "*stateless translog recovery*operationsRead=*";

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
                new MockLog.SeenEventExpectation(
                    "recovery summary",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.INFO,
                    RECOVERY_MESSAGE
                )
            );
            startIndexNode();
            ensureGreen(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }

    public void testRecoveryStillReportsWhenItReplayedNothing() throws Exception {
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

        // leave the shard nothing to replay, the shape that currently leaves no trace at all
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
                    "recovery summary with no operations",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.INFO,
                    "*stateless translog recovery*operationsRead=0*"
                )
            );
            startIndexNode();
            ensureGreen(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }
}
