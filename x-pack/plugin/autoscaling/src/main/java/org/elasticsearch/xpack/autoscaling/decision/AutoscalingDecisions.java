/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Represents a collection of individual autoscaling decisions that can be aggregated into a single autoscaling decision.
 */
public class AutoscalingDecisions implements ToXContent, Writeable {

    private final Collection<AutoscalingDecision> decisions;

    public Collection<AutoscalingDecision> decisions() {
        return decisions;
    }

    public AutoscalingDecisions(final Collection<AutoscalingDecision> decisions) {
        Objects.requireNonNull(decisions);
        if (decisions.isEmpty()) {
            throw new IllegalArgumentException("decisions can not be empty");
        }
        this.decisions = decisions;
    }

    public AutoscalingDecisions(final StreamInput in) throws IOException {
        this.decisions = in.readList(AutoscalingDecision::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeCollection(decisions);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("decision", type());
        builder.array("decisions", decisions.toArray());
        builder.endObject();
        return builder;
    }

    public AutoscalingDecisionType type() {
        if (decisions.stream().anyMatch(p -> p.type() == AutoscalingDecisionType.SCALE_UP)) {
            // if any deciders say to scale up
            return AutoscalingDecisionType.SCALE_UP;
        } else if (decisions.stream().allMatch(p -> p.type() == AutoscalingDecisionType.SCALE_DOWN)) {
            // if all deciders say to scale down
            return AutoscalingDecisionType.SCALE_DOWN;
        } else {
            // otherwise, do not scale
            return AutoscalingDecisionType.NO_SCALE;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AutoscalingDecisions that = (AutoscalingDecisions) o;
        return decisions.equals(that.decisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(decisions);
    }

}
