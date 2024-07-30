/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ingest.TestIngestDocument.emptyIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LogstashInternalBridgeTests extends ESTestCase {
    public void testIngestDocumentRerouteBridge() {
        final IngestDocument ingestDocument = emptyIngestDocument();
        ingestDocument.setFieldValue("_index", "nowhere");
        assertThat(ingestDocument.getFieldValue("_index", String.class), is(equalTo("nowhere")));
        assertThat(LogstashInternalBridge.isReroute(ingestDocument), is(false));

        ingestDocument.reroute("somewhere");
        assertThat(ingestDocument.getFieldValue("_index", String.class), is(equalTo("somewhere")));
        assertThat(LogstashInternalBridge.isReroute(ingestDocument), is(true));

        LogstashInternalBridge.resetReroute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("_index", String.class), is(equalTo("somewhere")));
        assertThat(LogstashInternalBridge.isReroute(ingestDocument), is(false));
    }

    public void testCreateThreadPool() {
        final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TEST").build();
        ThreadPool threadPool = null;
        try {
            threadPool = LogstashInternalBridge.createThreadPool(settings);
            assertThat(threadPool, is(notNullValue()));
        } finally {
            if (Objects.nonNull(threadPool)) {
                ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
            }
        }
    }
}
