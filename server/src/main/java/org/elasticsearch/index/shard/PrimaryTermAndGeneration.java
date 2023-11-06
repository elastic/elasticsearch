/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Comparator;

public record PrimaryTermAndGeneration(long primaryTerm, long generation) implements Writeable, Comparable<PrimaryTermAndGeneration> {

    private static final Comparator<PrimaryTermAndGeneration> COMPARATOR = Comparator.comparing(PrimaryTermAndGeneration::primaryTerm)
        .thenComparing(PrimaryTermAndGeneration::generation);

    public static final PrimaryTermAndGeneration ZERO = new PrimaryTermAndGeneration(0, 0);

    public PrimaryTermAndGeneration(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
    }

    @Override
    public String toString() {
        return "[primary term=" + primaryTerm + ", generation=" + generation + ']';
    }

    @Override
    public int compareTo(PrimaryTermAndGeneration other) {
        return COMPARATOR.compare(this, other);
    }
}
