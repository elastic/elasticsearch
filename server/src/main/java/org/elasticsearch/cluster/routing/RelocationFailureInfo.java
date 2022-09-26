/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Holds additional information as to why the shard failed to relocate.
 */
public record RelocationFailureInfo(int failedRelocations) implements ToXContentFragment, Writeable {

    public RelocationFailureInfo {
        assert failedRelocations >= 0 : "Expect non-negative failures count, got: " + failedRelocations;
    }

    public RelocationFailureInfo(StreamInput in) throws IOException {
        this(in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(failedRelocations);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("relocation_failure_info");
        if (failedRelocations > 0) {
            builder.field("failed_attempts", failedRelocations);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (failedRelocations > 0) {
            sb.append("failed_attempts[").append(failedRelocations).append("]");
        }
        return sb.toString();
    }
}
