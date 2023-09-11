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
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ElserMlNodeServiceSettings implements ServiceSettings {

    public static final String NAME = "elser_mlnode_service_settings";
    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";

    private final int numAllocations;
    private final int numThreads;

    public static ElserMlNodeServiceSettings fromMap(Map<String, Object> map) {
        Integer numAllocations = MapParsingUtils.removeAsType(map, NUM_ALLOCATIONS, Integer.class);
        Integer numThreads = MapParsingUtils.removeAsType(map, NUM_THREADS, Integer.class);

        if (numAllocations == null) {
            throw MapParsingUtils.missingSettingError(NUM_ALLOCATIONS, Model.SERVICE_SETTINGS);
        }
        if (numThreads == null) {
            throw MapParsingUtils.missingSettingError(NUM_THREADS, Model.SERVICE_SETTINGS);
        }

        return new ElserMlNodeServiceSettings(numAllocations, numThreads);
    }

    public ElserMlNodeServiceSettings(int numAllocations, int numThreads) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
    }

    public ElserMlNodeServiceSettings(StreamInput in) throws IOException {
        numAllocations = in.readVInt();
        numThreads = in.readVInt();
    }

    public int getNumAllocations() {
        return numAllocations;
    }

    public int getNumThreads() {
        return numThreads;
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
    public int hashCode() {
        // TODO Class has no members all instances are equivalent
        // Return the hash of NAME to make the serialization tests pass
        return Objects.hashCode(NAME);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElserMlNodeServiceSettings that = (ElserMlNodeServiceSettings) o;
        return numAllocations == that.numAllocations && numThreads == that.numThreads;
    }
}
