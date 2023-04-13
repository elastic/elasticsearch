/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record BlobLocation(long primaryTerm, String blobName, long offset, long length) implements Writeable {

    public BlobLocation(StreamInput streamInput) throws IOException {
        this(streamInput.readVLong(), streamInput.readString(), streamInput.readVLong(), streamInput.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeString(blobName);
        out.writeVLong(offset);
        out.writeVLong(length);
    }

    @Override
    public String toString() {
        return "BlobLocation{"
            + "primaryTerm="
            + primaryTerm
            + ", blobName='"
            + blobName
            + '\''
            + ", offset="
            + offset
            + ", length="
            + length
            + '}';
    }
}
