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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

/**
 * A class that contains all information related to a submitted async search.
 */
class AsyncSearchId {
    private final String indexName;
    private final String docId;
    private final TaskId taskId;
    private final String encoded;

    AsyncSearchId(String indexName, String docId, TaskId taskId) {
        this.indexName = indexName;
        this.docId = docId;
        this.taskId = taskId;
        this.encoded = encode(indexName, docId, taskId);
    }

    AsyncSearchId(String id) throws IOException {
        try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap( Base64.getDecoder().decode(id)))) {
            this.indexName = in.readString();
            this.docId = in.readString();
            this.taskId = new TaskId(in.readString());
            this.encoded = id;
        } catch (IOException e) {
            throw new IOException("invalid id: " + id);
        }
    }

    /**
     * The index name where to find the response if the task is not running.
     */
    String getIndexName() {
        return indexName;
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
        return indexName.equals(searchId.indexName) &&
            docId.equals(searchId.docId) &&
            taskId.equals(searchId.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, docId, taskId);
    }

    /**
     * Encode the informations needed to retrieve a async search response
     * in a base64 encoded string.
     */
    static String encode(String indexName, String docId, TaskId taskId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(indexName);
            out.writeString(docId);
            out.writeString(taskId.toString());
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Decode a base64 encoded string into an {@link AsyncSearchId} that can be used
     * to retrieve the response of an async search.
     */
    static AsyncSearchId decode(String id) throws IOException {
        try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap( Base64.getDecoder().decode(id)))) {
            return new AsyncSearchId(in.readString(), in.readString(), new TaskId(in.readString()));
        } catch (IOException e) {
            throw new IOException("invalid id: " + id);
        }
    }
}
