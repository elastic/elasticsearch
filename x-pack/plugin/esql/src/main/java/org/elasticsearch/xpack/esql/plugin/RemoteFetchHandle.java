/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Serializable row handle for coordinator-driven remote fetch.
 * <p>
 * The existing {@code _doc} column is intentionally local-only because {@code DocBlock}s do not cross a transport
 * boundary. This handle captures the minimum routing information needed to revisit a document on its owning node
 * after the coordinator has already narrowed the candidate set.
 */
public record RemoteFetchHandle(String nodeId, String sessionId, int shard, int segment, int doc) implements Writeable {
    public static final Writeable.Reader<RemoteFetchHandle> READER = RemoteFetchHandle::new;

    public RemoteFetchHandle {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(sessionId, "sessionId");
        if (shard < 0) {
            throw new IllegalArgumentException("shard must be non-negative");
        }
        if (segment < 0) {
            throw new IllegalArgumentException("segment must be non-negative");
        }
        if (doc < 0) {
            throw new IllegalArgumentException("doc must be non-negative");
        }
    }

    public RemoteFetchHandle(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readVInt(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(sessionId);
        out.writeVInt(shard);
        out.writeVInt(segment);
        out.writeVInt(doc);
    }

    /**
     * Encodes the handle into a transport-safe {@link BytesRef} so it can be carried through regular page blocks.
     */
    public BytesRef toBytesRef() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeTo(out);
            return BytesRef.deepCopyOf(out.bytes().toBytesRef());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode remote fetch handle", e);
        }
    }

    public static RemoteFetchHandle fromBytesRef(BytesRef bytesRef) {
        try (StreamInput in = StreamInput.wrap(BytesRef.deepCopyOf(bytesRef).bytes)) {
            return new RemoteFetchHandle(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode remote fetch handle", e);
        }
    }
}
