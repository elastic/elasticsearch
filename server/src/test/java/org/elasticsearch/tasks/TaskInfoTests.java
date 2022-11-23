/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TaskInfoTests extends AbstractXContentSerializingTestCase<TaskInfo> {

    @Override
    protected TaskInfo doParseInstance(XContentParser parser) {
        return TaskInfo.fromXContent(parser);
    }

    @Override
    protected TaskInfo createTestInstance() {
        return randomTaskInfo();
    }

    @Override
    protected Writeable.Reader<TaskInfo> instanceReader() {
        return TaskInfo::from;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new))
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // status and headers hold arbitrary content, we can't inject random fields in them
        return field -> "status".equals(field) || "headers".equals(field);
    }

    @Override
    protected TaskInfo mutateInstance(TaskInfo info) {
        switch (between(0, 9)) {
            case 0:
                TaskId taskId = new TaskId(info.taskId().getNodeId() + randomAlphaOfLength(5), info.taskId().getId());
                return new TaskInfo(
                    taskId,
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 1:
                return new TaskInfo(
                    info.taskId(),
                    info.type() + randomAlphaOfLength(5),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 2:
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action() + randomAlphaOfLength(5),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 3:
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description() + randomAlphaOfLength(5),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 4:
                Task.Status newStatus = randomValueOtherThan(info.status(), TaskInfoTests::randomRawTaskStatus);
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    newStatus,
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 5:
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime() + between(1, 100),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 6:
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos() + between(1, 100),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    info.headers()
                );
            case 7:
                // if not cancellable then mutate cancellable flag but leave cancelled flag unset
                // if cancelled then mutate cancelled flag but leave cancellable flag set
                // if cancellable but not cancelled then mutate exactly one of the flags
                //
                // cancellable | cancelled | random | cancellable == cancelled | isNowCancellable | isNowCancelled
                // false | false | - | true | true | false
                // true | true | - | true | true | false
                // true | false | false | false | false | false
                // true | false | true | false | true | true
                boolean isNowCancellable = info.cancellable() == info.cancelled() || randomBoolean();
                boolean isNowCancelled = isNowCancellable != (info.cancellable() == info.cancelled());
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    isNowCancellable,
                    isNowCancelled,
                    info.parentTaskId(),
                    info.headers()
                );
            case 8:
                TaskId parentId = new TaskId(info.parentTaskId().getNodeId() + randomAlphaOfLength(5), info.parentTaskId().getId());
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    parentId,
                    info.headers()
                );
            case 9:
                Map<String, String> headers = info.headers();
                if (headers == null) {
                    headers = Maps.newMapWithExpectedSize(1);
                } else {
                    headers = new HashMap<>(info.headers());
                }
                headers.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
                return new TaskInfo(
                    info.taskId(),
                    info.type(),
                    info.action(),
                    info.description(),
                    info.status(),
                    info.startTime(),
                    info.runningTimeNanos(),
                    info.cancellable(),
                    info.cancelled(),
                    info.parentTaskId(),
                    headers
                );
            default:
                throw new IllegalStateException();
        }
    }

    static TaskInfo randomTaskInfo() {
        TaskId taskId = randomTaskId();
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        Task.Status status = randomBoolean() ? randomRawTaskStatus() : null;
        String description = randomBoolean() ? randomAlphaOfLength(5) : null;
        long startTime = randomLong();
        long runningTimeNanos = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
            taskId,
            type,
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

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomLong());
    }

    private static RawTaskStatus randomRawTaskStatus() {
        try (XContentBuilder builder = XContentBuilder.builder(Requests.INDEX_CONTENT_TYPE.xContent())) {
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
}
