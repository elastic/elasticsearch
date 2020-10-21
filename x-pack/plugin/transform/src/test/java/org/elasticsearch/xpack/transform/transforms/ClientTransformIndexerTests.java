/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientTransformIndexerTests extends ESTestCase {

    public void testAudiOnFinishFrequency() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        ClientTransformIndexer indexer = new ClientTransformIndexer(
            mock(ThreadPool.class),
            ThreadPool.Names.GENERIC,
            mock(IndexBasedTransformConfigManager.class),
            mock(CheckpointProvider.class),
            new AtomicReference<>(IndexerState.STOPPED),
            null,
            mock(Client.class),
            mock(TransformAuditor.class),
            mock(TransformIndexerStats.class),
            mock(TransformConfig.class),
            Collections.emptyMap(),
            null,
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 0L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 2L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
            mock(TransformContext.class),
            false
        );

        List<Boolean> shouldAudit = IntStream.range(0, 100_000).boxed().map(indexer::shouldAuditOnFinish).collect(Collectors.toList());

        // Audit every checkpoint for the first 10
        assertTrue(shouldAudit.get(0));
        assertTrue(shouldAudit.get(1));
        assertTrue(shouldAudit.get(10));

        // Then audit every 10 while < 100
        assertFalse(shouldAudit.get(11));
        assertTrue(shouldAudit.get(20));
        assertFalse(shouldAudit.get(29));
        assertTrue(shouldAudit.get(30));
        assertFalse(shouldAudit.get(99));

        // Then audit every 100 < 1000
        assertTrue(shouldAudit.get(100));
        assertFalse(shouldAudit.get(109));
        assertFalse(shouldAudit.get(110));
        assertFalse(shouldAudit.get(199));

        // Then audit every 1000 for the rest of time
        assertFalse(shouldAudit.get(1999));
        assertFalse(shouldAudit.get(2199));
        assertTrue(shouldAudit.get(3000));
        assertTrue(shouldAudit.get(10_000));
        assertFalse(shouldAudit.get(10_999));
        assertTrue(shouldAudit.get(11_000));
        assertFalse(shouldAudit.get(11_001));
        assertFalse(shouldAudit.get(11_999));
    }

}
