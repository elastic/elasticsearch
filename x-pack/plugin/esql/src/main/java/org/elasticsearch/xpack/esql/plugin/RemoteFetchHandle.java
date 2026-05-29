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
 * {@code DocBlock} references (shard ordinal, segment, doc) are only meaningful on the node that produced them —
 * a coordinator receiving pages from multiple data nodes cannot resolve those local ordinals back to the original
 * shard. This handle pairs the doc triple with the originating node and retained session so the coordinator can route a
 * fetch request back to the owning node after narrowing the candidate set.
 * <p>
 * Remote fetch deliberately carries this as one serialized {@code keyword}-like value per row. A struct-of-arrays
 * representation would save repeated node/retained-session bytes, but it would require a new internal data type and block
 * implementation to keep the handle as a single logical column across generic projection, exchange, and top-N
 * operators. The serialized form keeps this prototype on existing block/exchange machinery.
 */
public record RemoteFetchHandle(String nodeId, String retainedSessionId, int shard, int segment, int doc) implements Writeable {
    public static final Writeable.Reader<RemoteFetchHandle> READER = RemoteFetchHandle::new;

    public RemoteFetchHandle {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(retainedSessionId, "retainedSessionId");
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
        out.writeString(retainedSessionId);
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
        try (StreamInput in = StreamInput.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length)) {
            return new RemoteFetchHandle(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode remote fetch handle", e);
        }
    }
}
