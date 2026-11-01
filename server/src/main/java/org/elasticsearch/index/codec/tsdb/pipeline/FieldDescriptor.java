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

/**
 * Versioned envelope for the {@link PipelineDescriptor} wire format.
 *
 * <p>Every {@link PipelineDescriptor} written to {@code .dvm} metadata is prefixed
 * with a format version VInt, enabling future format evolution without breaking
 * segment compatibility. Block offsets and value counts are managed by the
 * consumer/producer's {@code writeFieldEntry}/{@code readNumeric} flow.
 */
public final class FieldDescriptor {

    static final int CURRENT_FORMAT_VERSION = 1;

    private FieldDescriptor() {}

    /**
     * Writes a versioned pipeline descriptor to the metadata output.
     *
     * @param meta the metadata output stream
     * @param pipeline the pipeline descriptor to persist
     * @throws IOException if an I/O error occurs
     */
    public static void write(final DataOutput meta, final PipelineDescriptor pipeline) throws IOException {
        meta.writeVInt(CURRENT_FORMAT_VERSION);
        pipeline.writeTo(meta);
    }

    /**
     * Reads a versioned pipeline descriptor from the metadata input.
     *
     * @param meta the metadata input stream
     * @return the deserialized pipeline descriptor
     * @throws IOException if the format version is unsupported or an I/O error occurs
     */
    public static PipelineDescriptor read(final DataInput meta) throws IOException {
        final int formatVersion = meta.readVInt();
        if (formatVersion != CURRENT_FORMAT_VERSION) {
            throw new IOException(
                "Unsupported FieldDescriptor format version: "
                    + formatVersion
                    + ". Maximum supported version is "
                    + CURRENT_FORMAT_VERSION
                    + ". This may indicate data written by a newer version of Elasticsearch."
            );
        }
        return PipelineDescriptor.readFrom(meta);
    }
}
