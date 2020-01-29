/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

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
 * A class that contains all information related to a submitted async search.
 */
class AsyncSearchId {
    private final String docId;
    private final TaskId taskId;
    private final String encoded;

    AsyncSearchId(String docId, TaskId taskId) {
        this.docId = docId;
        this.taskId = taskId;
        this.encoded = encode(docId, taskId);
    }

    /**
     * The document id of the response in the index if the task is not running.
     */
    String getDocId() {
        return docId;
    }

    /**
     * The {@link TaskId} of the async search in the task manager.
     */
    TaskId getTaskId() {
        return taskId;
    }

    /**
     * Get the encoded string that represents this search.
     */
    String getEncoded() {
        return encoded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsyncSearchId searchId = (AsyncSearchId) o;
        return docId.equals(searchId.docId) &&
            taskId.equals(searchId.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, taskId);
    }

    @Override
    public String toString() {
        return "AsyncSearchId{" +
            "docId='" + docId + '\'' +
            ", taskId=" + taskId +
            '}';
    }

    /**
     * Encode the informations needed to retrieve a async search response
     * in a base64 encoded string.
     */
    static String encode(String docId, TaskId taskId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(docId);
            out.writeString(taskId.toString());
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Decode a base64 encoded string into an {@link AsyncSearchId} that can be used
     * to retrieve the response of an async search.
     */
    static AsyncSearchId decode(String id) {
        final AsyncSearchId searchId;
        try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(Base64.getUrlDecoder().decode(id)))) {
            searchId = new AsyncSearchId(in.readString(), new TaskId(in.readString()));
            if (in.available() > 0) {
                throw new IllegalArgumentException("invalid id:[" + id + "]");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid id:[" + id + "]");
        }
        return searchId;
    }
}
