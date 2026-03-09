/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

// NOTE: Versioned envelope for PipelineDescriptor wire format. Every PipelineDescriptor
// written to .dvm metadata is prefixed with a format version VInt, enabling future format
// evolution without breaking segment compatibility. Block offsets and value counts are
// managed by the consumer/producer's writeField/readNumeric flow.
public final class FieldDescriptor {

    static final int CURRENT_FORMAT_VERSION = 1;

    private FieldDescriptor() {}

    public static void write(DataOutput meta, PipelineDescriptor pipeline) throws IOException {
        meta.writeVInt(CURRENT_FORMAT_VERSION);
        pipeline.writeTo(meta);
    }

    public static PipelineDescriptor read(DataInput meta) throws IOException {
        final int formatVersion = meta.readVInt();
        return switch (formatVersion) {
            case 1 -> PipelineDescriptor.readFrom(meta);
            default -> throw new IOException(
                "Unsupported FieldDescriptor format version: "
                    + formatVersion
                    + ". Maximum supported version is "
                    + CURRENT_FORMAT_VERSION
                    + ". This may indicate data written by a newer version of Elasticsearch."
            );
        };
    }
}
