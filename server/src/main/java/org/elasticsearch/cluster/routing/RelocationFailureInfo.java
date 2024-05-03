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

    public static final RelocationFailureInfo NO_FAILURES = new RelocationFailureInfo(0);

    public RelocationFailureInfo {
        assert failedRelocations >= 0 : "Expect non-negative failures count, got: " + failedRelocations;
    }

    public static RelocationFailureInfo readFrom(StreamInput in) throws IOException {
        int failures = in.readVInt();
        return failures == 0 ? NO_FAILURES : new RelocationFailureInfo(failures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(failedRelocations);
    }

    public RelocationFailureInfo incFailedRelocations() {
        return new RelocationFailureInfo(failedRelocations + 1);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("relocation_failure_info");
        builder.field("failed_attempts", failedRelocations);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "failed_attempts[" + failedRelocations + "]";
    }
}
