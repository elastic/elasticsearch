/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientDataFrameIndexerTests extends ESTestCase {

    public void testAudiOnFinishFrequency() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));
        DataFrameTransformTask parentTask = new DataFrameTransformTask(1,
            "dataframe",
            "ptask",
            new TaskId("dataframe:1"),
            mock(DataFrameTransform.class),
            null,
            mock(SchedulerEngine.class),
            mock(DataFrameAuditor.class),
            threadPool,
            Collections.emptyMap());
        ClientDataFrameIndexer indexer = new ClientDataFrameIndexer(
            mock(DataFrameTransformsConfigManager.class),
            mock(CheckpointProvider.class),
            new AtomicReference<>(IndexerState.STOPPED),
            null,
            mock(Client.class),
            mock(DataFrameAuditor.class),
            mock(DataFrameIndexerTransformStats.class),
            mock(DataFrameTransformConfig.class),
            Collections.emptyMap(),
            null,
            new DataFrameTransformCheckpoint("transform",
                Instant.now().toEpochMilli(),
                0L,
                Collections.emptyMap(),
                Instant.now().toEpochMilli()),
            new DataFrameTransformCheckpoint("transform",
                Instant.now().toEpochMilli(),
                2L,
                Collections.emptyMap(),
                Instant.now().toEpochMilli()),
            parentTask);

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
