/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.task.ReindexDataStreamTask;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testNonExistentDataStream() {
        String nonExistentDataStreamName = randomAlphaOfLength(50);
        ReindexDataStreamRequest reindexDataStreamRequest = new ReindexDataStreamRequest(nonExistentDataStreamName);
        ReindexDataStreamResponse response = client().execute(
            new ActionType<ReindexDataStreamResponse>(ReindexDataStreamAction.NAME),
            reindexDataStreamRequest
        ).actionGet();
        String persistentTaskId = response.getTaskId();
        assertThat(persistentTaskId, equalTo("reindex-data-stream-" + nonExistentDataStreamName));
        AtomicReference<ReindexDataStreamTask> runningTask = new AtomicReference<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            TaskManager taskManager = transportService.getTaskManager();
            Map<Long, CancellableTask> tasksMap = taskManager.getCancellableTasks();
            Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getType().equals("persistent"))
                .filter(
                    entry -> entry.getValue() instanceof ReindexDataStreamTask
                        && persistentTaskId.equals((((ReindexDataStreamTask) entry.getValue()).getPersistentTaskId()))
                )
                .findAny();
            optionalTask.ifPresent(
                longCancellableTaskEntry -> runningTask.compareAndSet(null, (ReindexDataStreamTask) longCancellableTaskEntry.getValue())
            );
        }
        ReindexDataStreamTask task = runningTask.get();
        assertThat(task.getStatus().complete(), equalTo(true));
        assertThat(task.getStatus().exception().getCause().getMessage(), equalTo("no such index [" + nonExistentDataStreamName + "]"));
        assertNotNull(task);
    }
}
