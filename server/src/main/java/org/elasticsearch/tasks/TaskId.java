/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Task id that consists of node id and id of the task on the node
 */
public final class TaskId implements Writeable {

    public static final TaskId EMPTY_TASK_ID = new TaskId();

    private final String nodeId;
    private final long id;

    public TaskId(String nodeId, long id) {
        if (nodeId.isEmpty()) {
            throw new IllegalArgumentException("0 length nodeIds are reserved for EMPTY_TASK_ID and are otherwise invalid.");
        }
        this.nodeId = nodeId;
        this.id = id;
    }

    /**
     * Builds {@link #EMPTY_TASK_ID}.
     */
    private TaskId() {
        nodeId = "";
        id = -1;
    }

    public TaskId(String taskId) {
        if (Strings.hasLength(taskId) && "unset".equals(taskId) == false) {
            String[] s = Strings.split(taskId, ":");
            if (s == null || s.length != 2) {
                throw new IllegalArgumentException("malformed task id " + taskId);
            }
            this.nodeId = s[0];
            try {
                this.id = Long.parseLong(s[1]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("malformed task id " + taskId, ex);
            }
        } else {
            nodeId = "";
            id = -1L;
        }
    }

    /**
     * Read a {@linkplain TaskId} from a stream. {@linkplain TaskId} has this rather than the usual constructor that takes a
     * {@linkplain StreamInput} so we can return the {@link #EMPTY_TASK_ID} without allocating.
     */
    public static TaskId readFromStream(StreamInput in) throws IOException {
        String nodeId = in.readString();
        if (nodeId.isEmpty()) {
            /*
             * The only TaskId allowed to have the empty string as its nodeId is the EMPTY_TASK_ID and there is only ever one of it and it
             * never writes its taskId to save bytes on the wire because it is by far the most common TaskId.
             */
            return EMPTY_TASK_ID;
        }
        return new TaskId(nodeId, in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        if (nodeId.isEmpty()) {
            // Shortcut the EMPTY_TASK_ID, the only TaskId allowed to have the empty string as its nodeId.
            return;
        }
        out.writeLong(id);
    }

    public static ContextParser<Void, TaskId> parser() {
        return (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new TaskId(p.text());
            }
            throw new ElasticsearchParseException("Expected a string but found [{}] instead", p.currentToken());
        };
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getId() {
        return id;
    }

    public boolean isSet() {
        return id != -1L;
    }

    @Override
    public String toString() {
        if (isSet()) {
            return nodeId + ":" + id;
        } else {
            return "unset";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskId taskId = (TaskId) o;

        if (id != taskId.id) return false;
        return nodeId.equals(taskId.nodeId);

    }

    @Override
    public int hashCode() {
        int result = nodeId.hashCode();
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }
}
