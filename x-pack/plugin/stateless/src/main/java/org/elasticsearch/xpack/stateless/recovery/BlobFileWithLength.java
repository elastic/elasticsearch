/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.stateless.commits.BccBlob;
import org.elasticsearch.xpack.stateless.commits.Blob;

import java.io.IOException;

/// A [BccBlob] paired with its byte length
record BlobFileWithLength(BccBlob bccBlob, long length) implements Writeable {

    BlobFileWithLength(StreamInput in) throws IOException {
        this(Blob.readFrom(in, BccBlob.class), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        bccBlob.writeTo(out);
        out.writeVLong(length);
    }
}
