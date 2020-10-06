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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Represents a collection of individual autoscaling decisions that can be aggregated into a single autoscaling decision for a tier
 */
public class AutoscalingDecisions implements ToXContent, Writeable {

    private final String tier;
    private final AutoscalingCapacity currentCapacity;
    private final SortedMap<String, AutoscalingDecision> decisions;

    /**
     * Return map of decisions, keyed by decider name.
     */
    public Map<String, AutoscalingDecision> decisions() {
        return decisions;
    }

    public AutoscalingDecisions(
        final String tier,
        final AutoscalingCapacity currentCapacity,
        final SortedMap<String, AutoscalingDecision> decisions
    ) {
        Objects.requireNonNull(tier);
        Objects.requireNonNull(currentCapacity);
        this.tier = tier;
        this.currentCapacity = currentCapacity;
        Objects.requireNonNull(decisions);
        if (decisions.isEmpty()) {
            throw new IllegalArgumentException("decisions can not be empty");
        }
        this.decisions = decisions;
    }

    public AutoscalingDecisions(final StreamInput in) throws IOException {
        this.tier = in.readString();
        this.currentCapacity = new AutoscalingCapacity(in);
        this.decisions = new TreeMap<>(in.readMap(StreamInput::readString, AutoscalingDecision::new));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(tier);
        currentCapacity.writeTo(out);
        out.writeMap(decisions, StreamOutput::writeString, (output, decision) -> decision.writeTo(output));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("tier", tier);
        AutoscalingCapacity requiredCapacity = requiredCapacity();
        if (requiredCapacity != null) {
            builder.field("required_capacity", requiredCapacity);
        }
        builder.field("current_capacity", currentCapacity);
        builder.startArray("decisions");
        for (Map.Entry<String, AutoscalingDecision> entry : decisions.entrySet()) {
            builder.startObject();
            builder.field("name", entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public AutoscalingCapacity requiredCapacity() {
        if (decisions.values().stream().map(AutoscalingDecision::requiredCapacity).anyMatch(Objects::isNull)) {
            // any undetermined decider cancels out any decision making.
            return null;
        }
        Optional<AutoscalingCapacity> result = decisions.values()
            .stream()
            .map(AutoscalingDecision::requiredCapacity)
            .reduce(AutoscalingCapacity::upperBound);
        assert result.isPresent();
        return result.get();
    }

    public AutoscalingCapacity currentCapacity() {
        return currentCapacity;
    }

    public String tier() {
        return tier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingDecisions that = (AutoscalingDecisions) o;
        return tier.equals(that.tier) && currentCapacity.equals(that.currentCapacity) && decisions.equals(that.decisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier, currentCapacity, decisions);
    }
}
