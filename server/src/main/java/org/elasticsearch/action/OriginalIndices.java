/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Used to keep track of original indices within internal (e.g. shard level) requests
 */
public final class OriginalIndices implements IndicesRequest {

    //constant to use when original indices are not applicable and will not be serialized across the wire
    public static final OriginalIndices NONE = new OriginalIndices(null, null);

    private final String[] indices;
    private final IndicesOptions indicesOptions;

    public OriginalIndices(IndicesRequest indicesRequest) {
        this(indicesRequest.indices(), indicesRequest.indicesOptions());
    }

    public OriginalIndices(String[] indices, IndicesOptions indicesOptions) {
        this.indices = indices;
        this.indicesOptions = indicesOptions;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public static OriginalIndices readOriginalIndices(StreamInput in) throws IOException {
        return new OriginalIndices(in.readStringArray(), IndicesOptions.readIndicesOptions(in));
    }

    public static void writeOriginalIndices(OriginalIndices originalIndices, StreamOutput out) throws IOException {
        assert originalIndices != NONE;
        out.writeStringArrayNullable(originalIndices.indices);
        originalIndices.indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public String toString() {
        return "OriginalIndices{" +
            "indices=" + Arrays.toString(indices) +
            ", indicesOptions=" + indicesOptions +
            '}';
    }
}
