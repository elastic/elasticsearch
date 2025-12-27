/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

/**
 * A single chunk of fetch results streamed from a data node to the coordinator.
 * Contains sequence information to maintain correct ordering when chunks arrive out of order.
 **/
public record FetchPhaseResponseChunk(
    long timestampMillis,
    Type type,
    ShardId shardId,
    SearchHits hits,
    int from,
    int size,
    int expectedDocs,
    long sequenceStart  // Sequence number of first hit in this chunk
) implements Writeable {

    /**
     * The type of chunk being sent.
     */
    public enum Type {
        /**
         * Contains a batch of search hits. Multiple HITS chunks may be sent for a single
         * shard fetch operation.
         */
        HITS
    }

    /**
     * Compact constructor with validation.
     *
     * @throws IllegalArgumentException if shardIndex is invalid
     */
    public FetchPhaseResponseChunk {
        if (shardId.getId() < -1) {
            throw new IllegalArgumentException("invalid: " + this);
        }
    }

    /**
     * Deserializes a chunk from the given stream.
     *
     * @param in the stream to read from
     * @throws IOException if deserialization fails
     */
    public FetchPhaseResponseChunk(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readEnum(Type.class),
            new ShardId(in),
            readOptionalHits(in),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVLong()
        );
    }

    private static SearchHits readOptionalHits(StreamInput in) throws IOException {
        if (in.readBoolean() == false) {
            return null;
        }
        return SearchHits.readFrom(in, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestampMillis);
        out.writeEnum(type);
        shardId.writeTo(out);

        if (hits == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            hits.writeTo(out);
        }
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeVInt(expectedDocs);
        out.writeVLong(sequenceStart);
    }

    /**
     * Interface for sending chunk responses from the data node to the coordinator.
     * <p>
     * Implementations send chunks via {@link TransportFetchPhaseResponseChunkAction}.
     */
    public interface Writer {
        void writeResponseChunk(FetchPhaseResponseChunk responseChunk, ActionListener<Void> listener);
    }
}
