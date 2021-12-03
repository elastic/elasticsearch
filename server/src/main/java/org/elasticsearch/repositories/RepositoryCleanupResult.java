/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public final class RepositoryCleanupResult implements Writeable, ToXContentObject {

    public static final ObjectParser<RepositoryCleanupResult, Void> PARSER = new ObjectParser<>(
        RepositoryCleanupResult.class.getName(),
        true,
        RepositoryCleanupResult::new
    );

    private static final String DELETED_BLOBS = "deleted_blobs";

    private static final String DELETED_BYTES = "deleted_bytes";

    static {
        PARSER.declareLong((result, bytes) -> result.bytes = bytes, new ParseField(DELETED_BYTES));
        PARSER.declareLong((result, blobs) -> result.blobs = blobs, new ParseField(DELETED_BLOBS));
    }

    private long bytes;

    private long blobs;

    private RepositoryCleanupResult() {
        this(DeleteResult.ZERO);
    }

    public RepositoryCleanupResult(DeleteResult result) {
        this.blobs = result.blobsDeleted();
        this.bytes = result.bytesDeleted();
    }

    public RepositoryCleanupResult(StreamInput in) throws IOException {
        bytes = in.readLong();
        blobs = in.readLong();
    }

    public long bytes() {
        return bytes;
    }

    public long blobs() {
        return blobs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(bytes);
        out.writeLong(blobs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(DELETED_BYTES, bytes).field(DELETED_BLOBS, blobs).endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
