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
import java.util.Objects;

/**
 * Holds additional information as to why the shard failed to relocate.
 */
public class RelocationFailureInfo implements ToXContentFragment, Writeable {

    private final int failedRelocations;

    public RelocationFailureInfo(int failedRelocations) {
        this.failedRelocations = failedRelocations;
    }

    public RelocationFailureInfo(StreamInput in) throws IOException {
        this.failedRelocations = in.readVInt();
    }

    public int getFailedAllocations() {
        return failedRelocations;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelocationFailureInfo that = (RelocationFailureInfo) o;
        return failedRelocations == that.failedRelocations;
    }

    @Override
    public int hashCode() {
        return Objects.hash(failedRelocations);
    }
}
