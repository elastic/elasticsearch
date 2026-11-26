/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;

import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

public class EsqlLogProducerTests extends ESTestCase {
    private static final String PEAK_MEMORY_FIELD = EsqlLogProducer.PROFILE_PREFIX + "search_peak_memory_bytes";

    /**
     * A failed query logs no rollup counters, but the peak memory it held before it failed.
     */
    public void testFailedQueryLogsItsPeakMemory() {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM test | LIMIT 1");
        long peakMemoryBytes = randomLongBetween(1, Long.MAX_VALUE);
        request.setFailedPeakMemoryBytes(peakMemoryBytes);

        ESLogMessage message = produce(failureContext(request));

        assertThat(message.get(PEAK_MEMORY_FIELD), equalTo(Long.toString(peakMemoryBytes)));
        assertNull("a failed query has no rollup counters", message.get(EsqlLogProducer.PROFILE_PREFIX + "documents_found"));
    }

    /**
     * A query that failed before anything was measured logs no peak memory.
     */
    public void testFailedQueryWithoutMeasuredMemoryLogsNoPeak() {
        ESLogMessage message = produce(failureContext(syncEsqlQueryRequest("FROM test | LIMIT 1")));

        assertNull(message.get(PEAK_MEMORY_FIELD));
    }

    private EsqlCaptureContext failureContext(EsqlQueryRequest request) {
        Task task = new Task(1, "transport", EsqlQueryAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
        RuntimeException failure = new RuntimeException("query failed");
        if (randomBoolean()) {
            return new EsqlCaptureContextBuilder(task, request).build(failure);
        }
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(clusterAlias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        return new EsqlStreamCaptureContextBuilder(task, request, executionInfo, new PageStreamPublisher(), () -> null).build(failure);
    }

    private static ESLogMessage produce(EsqlCaptureContext context) {
        return new EsqlLogProducer().produce(context, new ActionLoggingFields(null) {}).orElseThrow();
    }
}
