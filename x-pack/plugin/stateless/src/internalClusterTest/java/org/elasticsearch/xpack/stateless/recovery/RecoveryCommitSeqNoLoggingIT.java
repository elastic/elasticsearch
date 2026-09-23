/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The commit a recovery starts from is normally superseded and deleted within minutes, so the sequence number range it
 * covered cannot be established afterwards unless it was written down at the time.
 */
public class RecoveryCommitSeqNoLoggingIT extends AbstractStatelessPluginIntegTestCase {

    public void testRecoveryLogsTheSeqNosOfTheCommitItStartsFrom() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();

        final String indexName = "commit-seqnos";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
        flush(indexName);

        internalCluster().stopNode(indexNode);

        try (var mockLog = MockLog.capture(StatelessIndexNodeRecoveryListener.class)) {
            // 20 documents occupy seq nos 0..19, so anything other than 19 here means the line is reporting a commit
            // other than the one being recovered from
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "recovery commit sequence numbers",
                    StatelessIndexNodeRecoveryListener.class.getCanonicalName(),
                    Level.INFO,
                    "*recovering from commit*local_checkpoint=19, max_seq_no=19*"
                )
            );
            startIndexNode();
            ensureGreen(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }
}
