/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;

import java.io.IOException;

/**
 * Wrapper class for the {@link DataStreamGlobalRetentionSettings}.
 */
public record DataStreamGlobalRetention(@Nullable TimeValue defaultRetention, @Nullable TimeValue maxRetention) implements Writeable {

    public static final NodeFeature GLOBAL_RETENTION = new NodeFeature("data_stream.lifecycle.global_retention");

    public static DataStreamGlobalRetention read(StreamInput in) throws IOException {
        return new DataStreamGlobalRetention(in.readOptionalTimeValue(), in.readOptionalTimeValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(defaultRetention);
        out.writeOptionalTimeValue(maxRetention);
    }

    @Override
    public String toString() {
        return "DataStreamGlobalRetention{"
            + "defaultRetention="
            + (defaultRetention == null ? "null" : defaultRetention.getStringRep())
            + ", maxRetention="
            + (maxRetention == null ? "null" : maxRetention.getStringRep())
            + '}';
    }
}
