/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class GetMigrationReindexStatusActionResponseTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        try {
            return new Response(randomTaskResult());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        return createTestInstance(); // There's only one field
    }

    private static TaskResult randomTaskResult() throws IOException {
        return switch (between(0, 2)) {
            case 0 -> new TaskResult(randomBoolean(), randomTaskInfo());
            case 1 -> new TaskResult(randomTaskInfo(), new RuntimeException("error"));
            case 2 -> new TaskResult(randomTaskInfo(), randomTaskResponse());
            default -> throw new UnsupportedOperationException("Unsupported random TaskResult constructor");
        };
    }

    static TaskInfo randomTaskInfo() {
        String nodeId = randomAlphaOfLength(5);
        TaskId taskId = randomTaskId(nodeId);
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        Task.Status status = randomBoolean() ? randomRawTaskStatus() : null;
        String description = randomBoolean() ? randomAlphaOfLength(5) : null;
        long startTime = randomLong();
        long runningTimeNanos = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(randomAlphaOfLength(5));
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
            taskId,
            type,
            nodeId,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers
        );
    }

    private static TaskId randomTaskId(String nodeId) {
        return new TaskId(nodeId, randomLong());
    }

    private static RawTaskStatus randomRawTaskStatus() {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            int fields = between(0, 10);
            for (int f = 0; f < fields; f++) {
                builder.field(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            builder.endObject();
            return new RawTaskStatus(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ToXContent randomTaskResponse() {
        Map<String, String> result = new TreeMap<>();
        int fields = between(0, 10);
        for (int f = 0; f < fields; f++) {
            result.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return (builder, params) -> {
            for (Map.Entry<String, String> entry : result.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            return builder;
        };
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        // return new NamedWriteableRegistry(List.of(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME,
        // RawTaskStatus::new)));
    }
}
