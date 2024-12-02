/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The statistics for a specific blob-store action
 *
 * @param operations The number of calls
 * @param requests The number of calls (including retries)
 */
public record BlobStoreActionStats(long operations, long requests) implements Writeable, ToXContentObject {

    public static BlobStoreActionStats ZERO = new BlobStoreActionStats(0, 0);

    public BlobStoreActionStats(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    public BlobStoreActionStats {
        assert operations >= 0 && requests >= 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(operations);
        out.writeLong(requests);
    }

    public BlobStoreActionStats add(BlobStoreActionStats other) {
        return new BlobStoreActionStats(Math.addExact(operations, other.operations), Math.addExact(requests, other.requests));
    }

    public boolean isZero() {
        return operations == 0 && requests == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BlobStoreActionStats other) {
            return requests == other.requests && operations == other.operations;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operations, requests);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("operations", operations);
        builder.field("requests", requests);
        builder.endObject();
        return builder;
    }
}
