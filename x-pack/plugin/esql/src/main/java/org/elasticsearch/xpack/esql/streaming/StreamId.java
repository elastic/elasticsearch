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
 * The id of a stored ES|QL stream: one {@link #RESERVED reserved} byte, the {@link TaskId} of the task that writes the results of a stream
 * query to the stream system index, and a random key, encoded as URL-safe Base64.
 * <p>
 * The task id leads to the node and the task of a stream that is still being written, without reading the storage. The random key makes
 * the id unique, because task numbers start again when a node restarts.
 * <p>
 * An id has exactly one {@link #encoded()} form, which is the key of the stream in the {@link StreamStorage}.
 */
public final class StreamId {

    /**
     * The first byte of the encoded form. It has no meaning yet: it is always {@code 0}, and {@link #decode} rejects any other value.
     */
    static final byte RESERVED = 0;

    private final TaskId taskId;
    private final String streamKey;
    private final String encoded;

    /**
     * @param taskId    the task that writes the results of a stream query to the stream system index
     * @param streamKey a random value that makes the id unique
     */
    public StreamId(TaskId taskId, String streamKey) {
        if (Strings.hasLength(streamKey) == false) {
            throw new IllegalArgumentException("the stream key must not be empty");
        }
        if (Objects.requireNonNull(taskId).isSet() == false) {
            throw new IllegalArgumentException("the task id must be set");
        }
        // A task id parsed from a string like [:1] is set but names no node, and a client can send one.
        if (taskId.getNodeId().isEmpty()) {
            throw new IllegalArgumentException("the node id of the task must not be empty");
        }
        // The task id is written as [node:number] and read back by splitting at the colon.
        if (taskId.getNodeId().indexOf(':') >= 0) {
            throw new IllegalArgumentException("the node id of the task must not contain [:]");
        }
        this.taskId = taskId;
        this.streamKey = streamKey;
        this.encoded = encode(taskId, streamKey);
    }

    /** The task that writes the results of a stream query to the stream system index. Its node id is the node the task runs on. */
    public TaskId taskId() {
        return taskId;
    }

    /** The random part that makes the id unique. */
    public String streamKey() {
        return streamKey;
    }

    /** The string that clients see and that the {@link StreamStorage} is keyed by. */
    public String encoded() {
        return encoded;
    }

    private static String encode(TaskId taskId, String streamKey) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte(RESERVED);
            out.writeString(taskId.toString());
            out.writeString(streamKey);
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
                byte reserved = in.readByte();
                if (reserved != RESERVED) {
                    throw new IllegalArgumentException("unexpected value of the reserved byte [" + reserved + "]");
                }
                String taskId = in.readString();
                String streamKey = in.readString();
                if (in.available() > 0) {
                    throw new IllegalArgumentException("unexpected trailing bytes");
                }
                decoded = new StreamId(new TaskId(taskId), streamKey);
            }
        } catch (Exception e) {
            // Whatever a client sends must end here: bad Base64, a reserved byte that is set, a truncated string, an invalid part.
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        if (decoded.encoded.equals(id) == false) {
            // For instance other padding. Accepting it would give one stream several ids, while the storage knows one.
            throw new IllegalArgumentException(
                "invalid id: [" + id + "]",
                new IllegalArgumentException("not the one encoded form of the id")
            );
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
