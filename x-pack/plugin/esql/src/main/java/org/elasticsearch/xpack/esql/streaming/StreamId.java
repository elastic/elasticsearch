/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.common.Strings;
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
 * The id of a stored ES|QL stream, the counterpart of {@code AsyncExecutionId}.
 * <p>
 * Like the async id it carries the {@link TaskId} of the query, so an action that must reach the running query (a delete, which
 * cancels it) finds the node and the task without reading the storage. The other part is a random key that makes the id unique:
 * task numbers start again when a node restarts, so the task id alone could name two streams.
 * <p>
 * {@code AsyncExecutionId} is not reused because its first part is the id of the single document an async result is stored in.
 * A stream is stored as several documents, so its first part names the stream, not a document.
 * <p>
 * The {@link StreamStorage} is keyed by the {@link #encoded()} string. An id has exactly one encoded form: {@link #decode} rejects
 * a string that decodes to the same parts but is written differently, so two different strings never name the same stream.
 */
public final class StreamId {

    private final String streamKey;
    private final TaskId taskId;
    private final String encoded;

    /**
     * @param streamKey a random value that makes the id unique, see {@code UUIDs#randomBase64UUID()}
     * @param taskId    the task of the query, on the node that coordinates it
     */
    public StreamId(String streamKey, TaskId taskId) {
        if (Strings.hasLength(streamKey) == false) {
            throw new IllegalArgumentException("the stream key must not be empty");
        }
        if (Objects.requireNonNull(taskId).isSet() == false) {
            throw new IllegalArgumentException("the task id must be set");
        }
        this.streamKey = streamKey;
        this.taskId = taskId;
        this.encoded = encode(streamKey, taskId);
    }

    /** The random part that makes the id unique. */
    public String streamKey() {
        return streamKey;
    }

    /** The task of the query. Its node id is the node that coordinates the query. */
    public TaskId taskId() {
        return taskId;
    }

    /** The string that clients see and that the {@link StreamStorage} is keyed by. */
    public String encoded() {
        return encoded;
    }

    private static String encode(String streamKey, TaskId taskId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(streamKey);
            out.writeString(taskId.toString());
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Decodes the string a client sent.
     *
     * @throws IllegalArgumentException if the string is not the encoded form of a stream id
     */
    public static StreamId decode(String id) {
        final StreamId decoded;
        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
            try (StreamInput in = new ByteBufferStreamInput(byteBuffer)) {
                String streamKey = in.readString();
                String taskId = in.readString();
                if (in.available() > 0) {
                    throw new IllegalArgumentException("unexpected trailing bytes");
                }
                decoded = new StreamId(streamKey, new TaskId(taskId));
            }
        } catch (Exception e) {
            // Whatever a client sends must end here: bad base64, a truncated string, a malformed task id.
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        if (decoded.encoded.equals(id) == false) {
            // For instance other padding. Accepting it would give one stream several ids, while the storage knows one.
            throw new IllegalArgumentException("invalid id: [" + id + "]");
        }
        return decoded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamId other = (StreamId) o;
        return streamKey.equals(other.streamKey) && taskId.equals(other.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamKey, taskId);
    }

    @Override
    public String toString() {
        return "StreamId{streamKey='" + streamKey + "', taskId=" + taskId + '}';
    }
}
