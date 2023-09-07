/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.TaskSettings;

import java.io.IOException;
import java.util.Objects;

public class ElserTaskSettings implements TaskSettings {

    public static final String NAME = "elser_sparse_embedding";
    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";

    public static ElserTaskSettings DEFAULT = new ElserTaskSettings(1, 1);

    private final int numAllocations;
    private final int numThreads;

    public ElserTaskSettings(int numAllocations, int numThreads) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
    }

    public ElserTaskSettings(StreamInput in) throws IOException {
        numAllocations = in.readVInt();
        numThreads = in.readVInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_ALLOCATIONS, numAllocations);
        builder.field(NUM_THREADS, numThreads);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_500_072;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numAllocations);
        out.writeVInt(numThreads);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElserTaskSettings that = (ElserTaskSettings) o;
        return numAllocations == that.numAllocations && numThreads == that.numThreads;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads);
    }
}
