/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TaskInfoTests extends AbstractSerializingTestCase<TaskInfo> {

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
        return TaskInfo::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        //status and headers hold arbitrary content, we can't inject random fields in them
        return field -> "status".equals(field) || "headers".equals(field);
    }

    @Override
    protected TaskInfo mutateInstance(TaskInfo info) {
        switch (between(0, 9)) {
            case 0:
                TaskId taskId = new TaskId(info.getTaskId().getNodeId() + randomAlphaOfLength(5), info.getTaskId().getId());
                return new TaskInfo(
                        taskId,
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 1:
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType() + randomAlphaOfLength(5),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 2:
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction() + randomAlphaOfLength(5),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 3:
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription() + randomAlphaOfLength(5),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 4:
                Task.Status newStatus = randomValueOtherThan(info.getStatus(), TaskInfoTests::randomRawTaskStatus);
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        newStatus,
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 5:
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime() + between(1, 100),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 6:
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos() + between(1, 100),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        info.getHeaders());
            case 7:
                // if not cancellable then mutate cancellable flag but leave cancelled flag unset
                // if cancelled then mutate cancelled flag but leave cancellable flag set
                // if cancellable but not cancelled then mutate exactly one of the flags
                //
                // cancellable | cancelled | random | cancellable == cancelled | isNowCancellable | isNowCancelled
                // false       | false     | -      | true                     | true             | false
                // true        | true      | -      | true                     | true             | false
                // true        | false     | false  | false                    | false            | false
                // true        | false     | true   | false                    | true             | true
                boolean isNowCancellable = info.isCancellable() == info.isCancelled() || randomBoolean();
                boolean isNowCancelled = isNowCancellable != (info.isCancellable() == info.isCancelled());
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        isNowCancellable,
                        isNowCancelled,
                        info.getParentTaskId(),
                        info.getHeaders());
            case 8:
                TaskId parentId = new TaskId(info.getParentTaskId().getNodeId() + randomAlphaOfLength(5), info.getParentTaskId().getId());
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        parentId,
                        info.getHeaders());
            case 9:
                Map<String, String> headers = info.getHeaders();
                if (headers == null) {
                    headers = new HashMap<>(1);
                } else {
                    headers = new HashMap<>(info.getHeaders());
                }
                headers.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
                return new TaskInfo(
                        info.getTaskId(),
                        info.getType(),
                        info.getAction(),
                        info.getDescription(),
                        info.getStatus(),
                        info.getStartTime(),
                        info.getRunningTimeNanos(),
                        info.isCancellable(),
                        info.isCancelled(),
                        info.getParentTaskId(),
                        headers);
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
        long runningTimeNanos = randomLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        Map<String, String> headers = randomBoolean() ?
                Collections.emptyMap() :
                Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
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
                headers);
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
