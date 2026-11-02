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
 * A failure in one search phase that is not bound to a shard and does not fail the search.
 * The search response still carries results, and lists these failures so that clients can tell
 * the results are degraded, for example that the hits were not reranked.
 */
public record SearchPhaseFailure(String phase, Exception cause) implements Writeable, ToXContentObject {

    public static final String PHASE_FIELD = "phase";
    public static final String REASON_FIELD = "reason";

    public SearchPhaseFailure(StreamInput in) throws IOException {
        this(in.readString(), in.readException());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(phase);
        out.writeException(cause);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PHASE_FIELD, phase);
        builder.startObject(REASON_FIELD);
        ElasticsearchException.generateThrowableXContent(builder, params, cause);
        builder.endObject();
        return builder.endObject();
    }
}
