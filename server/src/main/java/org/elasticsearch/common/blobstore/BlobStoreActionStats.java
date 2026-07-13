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

/**
 * The statistics for a specific blob-store action
 *
 * @param operations The number of calls
 * @param requests The number of calls (including retries)
 */
public record BlobStoreActionStats(long operations, long requests) implements Writeable, ToXContentObject {

    public static final BlobStoreActionStats ZERO = new BlobStoreActionStats(0, 0);

    public BlobStoreActionStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong());
    }

    public BlobStoreActionStats {
        assert operations >= 0 && requests >= 0 : "Requests (" + requests + ") and operations (" + operations + ") must be non-negative";
        // TODO: assert that requests >= operations once https://elasticco.atlassian.net/browse/ES-10223 is played
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(operations);
        out.writeVLong(requests);
    }

    public BlobStoreActionStats add(BlobStoreActionStats other) {
        return new BlobStoreActionStats(Math.addExact(operations, other.operations), Math.addExact(requests, other.requests));
    }

    public boolean isZero() {
        return operations == 0 && requests == 0;
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
