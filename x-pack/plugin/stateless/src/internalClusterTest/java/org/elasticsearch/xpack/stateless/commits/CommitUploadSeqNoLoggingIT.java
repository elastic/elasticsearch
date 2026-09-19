/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A commit records the sequence numbers it covered, and is then normally superseded and deleted within minutes. Unless
 * the range is written down as the commit is uploaded, there is afterwards no record of which operations it held.
 */
public class CommitUploadSeqNoLoggingIT extends AbstractStatelessPluginIntegTestCase {

    public void testUploadLogsTheSeqNosTheCommitCovered() throws Exception {
        startMasterOnlyNode();
        startIndexNode();

        final String indexName = "commit-upload";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();

        try (var mockLog = MockLog.capture(BatchedCompoundCommitUploadTask.class)) {
            // 20 documents occupy seq nos 0..19, so the commit that captures them must report 19 for both
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "commit upload sequence numbers",
                    BatchedCompoundCommitUploadTask.class.getCanonicalName(),
                    Level.INFO,
                    "*uploaded*local_checkpoint [19], max_seq_no [19]*"
                )
            );
            indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
            flush(indexName);
            mockLog.awaitAllExpectationsMatched();
        }
    }
}
