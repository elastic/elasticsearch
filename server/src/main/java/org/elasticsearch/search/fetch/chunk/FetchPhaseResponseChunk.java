/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;// package org.elasticsearch.search.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;

/**
 * Data node streams hits back using many small chunk requests.
 * DTO that carries each chunk of fetch results
 **/
public record FetchPhaseResponseChunk(
    long timestampMillis,
    Type type,
    int shardIndex,
    ShardSearchContextId contextId,
    SearchHits hits,
    int from,
    int size,
    int expectedDocs
) implements Writeable {

    public enum Type {
        START_RESPONSE,
        HITS,
    }

    public FetchPhaseResponseChunk {
        if (shardIndex < -1) {
            throw new IllegalArgumentException("invalid: " + this);
        }
    }

    public FetchPhaseResponseChunk(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readEnum(Type.class),
            in.readVInt(),
            in.readOptionalWriteable(ShardSearchContextId::new),
            readOptionalHits(in),
            in.readVInt(),
            in.readVInt(),
            in.readVInt()
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
        out.writeVInt(shardIndex);
        out.writeOptionalWriteable(contextId);


        // hits (optional)
        if (hits == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            hits.writeTo(out);
        }
        //out.writeOptionalWriteable(hits);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeVInt(expectedDocs);
    }


    public interface Writer {
        void writeResponseChunk(FetchPhaseResponseChunk responseChunk, ActionListener<Void> listener);
    }
}
