/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * client side counterpart of server side
 * <p>
 * {@link org.elasticsearch.tasks.TaskInfo}
 */
public class TaskInfo {

    private TaskId taskId;
    private String type;
    private String action;
    private String description;
    private long startTime;
    private long runningTimeNanos;
    private boolean cancellable;
    private boolean cancelled;
    private TaskId parentTaskId;
    private final Map<String, Object> status = new HashMap<>();
    private final Map<String, String> headers = new HashMap<>();

    public TaskInfo(TaskId taskId) {
        this.taskId = taskId;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public String getNodeId() {
        return taskId.nodeId;
    }

    public String getType() {
        return type;
    }

    void setType(String type) {
        this.type = type;
    }

    public String getAction() {
        return action;
    }

    void setAction(String action) {
        this.action = action;
    }

    public String getDescription() {
        return description;
    }

    void setDescription(String description) {
        this.description = description;
    }

    public long getStartTime() {
        return startTime;
    }

    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getRunningTimeNanos() {
        return runningTimeNanos;
    }

    void setRunningTimeNanos(long runningTimeNanos) {
        this.runningTimeNanos = runningTimeNanos;
    }

    public boolean isCancellable() {
        return cancellable;
    }

    void setCancellable(boolean cancellable) {
        this.cancellable = cancellable;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public TaskId getParentTaskId() {
        return parentTaskId;
    }

    void setParentTaskId(String parentTaskId) {
        this.parentTaskId = new TaskId(parentTaskId);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    void setHeaders(Map<String, String> headers) {
        this.headers.putAll(headers);
    }

    void setStatus(Map<String, Object> status) {
        this.status.putAll(status);
    }

    public Map<String, Object> getStatus() {
        return status;
    }

    private void noOpParse(Object s) {}

    public static final ObjectParser.NamedObjectParser<TaskInfo, Void> PARSER;

    static {
        ObjectParser<TaskInfo, Void> parser = new ObjectParser<>("tasks", true, null);
        // already provided in constructor: triggering a no-op
        parser.declareString(TaskInfo::noOpParse, new ParseField("node"));
        // already provided in constructor: triggering a no-op
        parser.declareLong(TaskInfo::noOpParse, new ParseField("id"));
        parser.declareString(TaskInfo::setType, new ParseField("type"));
        parser.declareString(TaskInfo::setAction, new ParseField("action"));
        parser.declareObject(TaskInfo::setStatus, (p, c) -> p.map(), new ParseField("status"));
        parser.declareString(TaskInfo::setDescription, new ParseField("description"));
        parser.declareLong(TaskInfo::setStartTime, new ParseField("start_time_in_millis"));
        parser.declareLong(TaskInfo::setRunningTimeNanos, new ParseField("running_time_in_nanos"));
        parser.declareBoolean(TaskInfo::setCancellable, new ParseField("cancellable"));
        parser.declareBoolean(TaskInfo::setCancelled, new ParseField("cancelled"));
        parser.declareString(TaskInfo::setParentTaskId, new ParseField("parent_task_id"));
        parser.declareObject(TaskInfo::setHeaders, (p, c) -> p.mapStrings(), new ParseField("headers"));
        PARSER = (XContentParser p, Void v, String name) -> parser.parse(p, new TaskInfo(new TaskId(name)), null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof TaskInfo) == false) return false;
        TaskInfo taskInfo = (TaskInfo) o;
        return getStartTime() == taskInfo.getStartTime() &&
            getRunningTimeNanos() == taskInfo.getRunningTimeNanos() &&
            isCancellable() == taskInfo.isCancellable() &&
            isCancelled() == taskInfo.isCancelled() &&
            Objects.equals(getTaskId(), taskInfo.getTaskId()) &&
            Objects.equals(getType(), taskInfo.getType()) &&
            Objects.equals(getAction(), taskInfo.getAction()) &&
            Objects.equals(getDescription(), taskInfo.getDescription()) &&
            Objects.equals(getParentTaskId(), taskInfo.getParentTaskId()) &&
            Objects.equals(status, taskInfo.status) &&
            Objects.equals(getHeaders(), taskInfo.getHeaders());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getTaskId(),
                getType(),
                getAction(),
                getDescription(),
                getStartTime(),
                getRunningTimeNanos(),
                isCancellable(),
                isCancelled(),
                getParentTaskId(),
                status,
                getHeaders()
        );
    }


    @Override
    public String toString() {
        return "TaskInfo{" +
            "taskId=" + taskId +
            ", type='" + type + '\'' +
            ", action='" + action + '\'' +
            ", description='" + description + '\'' +
            ", startTime=" + startTime +
            ", runningTimeNanos=" + runningTimeNanos +
            ", cancellable=" + cancellable +
            ", cancelled=" + cancelled +
            ", parentTaskId=" + parentTaskId +
            ", status=" + status +
            ", headers=" + headers +
            '}';
    }
}
