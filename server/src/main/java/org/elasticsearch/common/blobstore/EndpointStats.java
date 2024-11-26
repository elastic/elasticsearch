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

import java.io.IOException;
import java.util.Objects;

/**
 * The number of calls to an endpoint
 *
 * @param operations The number of calls
 * @param requests The number of calls (including retries)
 * @param legacyValue The number used before the migration to separate counts (sometimes operations, sometimes requests, for temporary BWC)
 */
public record EndpointStats(long operations, long requests, long legacyValue) implements Writeable {

    public static EndpointStats ZERO = new EndpointStats(0, 0, 0);

    public EndpointStats(long legacyValue) {
        this(-1, -1, legacyValue);
    }

    public EndpointStats(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(operations);
        out.writeLong(requests);
        out.writeLong(legacyValue);
    }

    public boolean isLegacyStats() {
        return operations == -1 && requests == -1;
    }

    public EndpointStats add(EndpointStats other) {
        if (isLegacyStats() || other.isLegacyStats()) {
            return new EndpointStats(legacyValue + other.legacyValue);
        } else {
            return new EndpointStats(operations + other.operations, requests + other.requests, legacyValue + other.legacyValue);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof EndpointStats other) {
            return requests == other.requests && operations == other.operations && legacyValue == other.legacyValue;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operations, requests, legacyValue);
    }
}
