/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Contains information on a failure of a particular search phase (ie not a shard failure),
 * that does not cause the search operation itself to fail.
 */
public record PhaseFailure(String phase, Exception failure) implements ToXContentObject, Writeable {
    public static final String PHASE_FIELD = "phase";
    public static final String FAILURE_FIELD = "failure";

    public static final PhaseFailure[] EMPTY_ARRAY = new PhaseFailure[0];

    public PhaseFailure(StreamInput in) throws IOException {
        this(in.readString(), in.readException());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(phase);
        out.writeException(failure);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PHASE_FIELD, phase);
        builder.startObject(FAILURE_FIELD);
        ElasticsearchException.generateThrowableXContent(builder, params, failure);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
