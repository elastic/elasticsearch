/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

/**
 * A class that contains all information related to a submitted async execution.
 */
public final class AsyncExecutionId {
    private final String docId;
    private final TaskId taskId;
    private final String encoded;

    public AsyncExecutionId(String docId, TaskId taskId) {
        this(docId, taskId, encode(docId, taskId));
    }

    private AsyncExecutionId(String docId, TaskId taskId, String encoded) {
        this.docId = docId;
        this.taskId = taskId;
        this.encoded = encoded;
    }

    /**
     * The document id of the response in the index if the task is not running.
     */
    public String getDocId() {
        return docId;
    }

    /**
     * The {@link TaskId} of the async execution in the task manager.
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * Gets the encoded string that represents this execution.
     */
    public String getEncoded() {
        return encoded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsyncExecutionId searchId = (AsyncExecutionId) o;
        return docId.equals(searchId.docId) && taskId.equals(searchId.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, taskId);
    }

    @Override
    public String toString() {
        return "AsyncExecutionId{" + "docId='" + docId + '\'' + ", taskId=" + taskId + '}';
    }

    /**
     * Encodes the informations needed to retrieve a async response
     * in a base64 encoded string.
     */
    public static String encode(String docId, TaskId taskId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(docId);
            out.writeString(taskId.toString());
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Decodes a base64 encoded string into an {@link AsyncExecutionId} that can be used
     * to retrieve the response of an async execution.
     */
    public static AsyncExecutionId decode(String id) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        String docId;
        String taskId;
        try (StreamInput in = new ByteBufferStreamInput(byteBuffer)) {
            docId = in.readString();
            taskId = in.readString();
            if (in.available() > 0) {
                throw new IllegalArgumentException("invalid id: [" + id + "]");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        return new AsyncExecutionId(docId, new TaskId(taskId), id);
    }
}
