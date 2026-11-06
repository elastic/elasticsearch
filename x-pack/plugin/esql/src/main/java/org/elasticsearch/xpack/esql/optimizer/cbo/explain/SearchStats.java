/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * How much work the plan search did in one run, so a reader can tell how close it came to its budget and what it cost.
 */
public record SearchStats(int groups, int candidates, int candidateBudget, int counterfactualSearches, long tookNanos)
    implements
        Writeable,
        ToXContentObject {

    public SearchStats(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt(), in.readVInt(), in.readVInt(), in.readZLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(groups);
        out.writeVInt(candidates);
        out.writeVInt(candidateBudget);
        out.writeVInt(counterfactualSearches);
        out.writeZLong(tookNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("groups", groups);
        b.field("candidates", candidates);
        b.field("candidate_budget", candidateBudget);
        b.field("counterfactual_searches", counterfactualSearches);
        b.field("took_nanos", tookNanos);
        // TimeValue rejects negative durations
        if (b.humanReadable() && tookNanos >= 0) {
            b.field("took", TimeValue.timeValueNanos(tookNanos).toString());
        }
        return b.endObject();
    }
}
