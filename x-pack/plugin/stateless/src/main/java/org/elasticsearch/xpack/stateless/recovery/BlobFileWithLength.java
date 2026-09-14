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
import org.elasticsearch.xpack.stateless.commits.BlobFile;

import java.io.IOException;

/// A [BlobFile] paired with its byte length
record BlobFileWithLength(BlobFile blobFile, long length) implements Writeable {

    BlobFileWithLength(StreamInput in) throws IOException {
        this(new BlobFile(in), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        blobFile.writeTo(out);
        out.writeVLong(length);
    }
}
