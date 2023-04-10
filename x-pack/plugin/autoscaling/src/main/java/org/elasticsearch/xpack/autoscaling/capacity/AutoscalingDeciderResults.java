/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;

/**
 * Represents a collection of individual autoscaling decider results that can be aggregated into a single autoscaling capacity for a
 * policy
 */
public class AutoscalingDeciderResults implements ToXContentObject, Writeable {

    private final AutoscalingCapacity currentCapacity;
    private final SortedSet<DiscoveryNode> currentNodes;
    private final SortedMap<String, AutoscalingDeciderResult> results;

    /**
     * Return map of results, keyed by decider name.
     */
    public Map<String, AutoscalingDeciderResult> results() {
        return results;
    }

    public AutoscalingDeciderResults(
        final AutoscalingCapacity currentCapacity,
        final SortedSet<DiscoveryNode> currentNodes,
        final SortedMap<String, AutoscalingDeciderResult> results
    ) {
        Objects.requireNonNull(currentCapacity);
        this.currentCapacity = currentCapacity;
        this.currentNodes = Objects.requireNonNull(currentNodes);
        Objects.requireNonNull(results);
        if (results.isEmpty()) {
            throw new IllegalArgumentException("results can not be empty");
        }
        this.results = results;
    }

    public AutoscalingDeciderResults(final StreamInput in) throws IOException {
        this.currentCapacity = new AutoscalingCapacity(in);
        this.currentNodes = in.readSet(DiscoveryNode::new)
            .stream()
            .collect(Collectors.toCollection(() -> new TreeSet<>(DISCOVERY_NODE_COMPARATOR)));
        this.results = new TreeMap<>(in.readMap(StreamInput::readString, AutoscalingDeciderResult::new));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        currentCapacity.writeTo(out);
        out.writeCollection(currentNodes);
        out.writeMap(results, StreamOutput::writeString, (output, result) -> result.writeTo(output));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        AutoscalingCapacity requiredCapacity = requiredCapacity();
        if (requiredCapacity != null) {
            builder.field("required_capacity", requiredCapacity);
        }
        builder.field("current_capacity", currentCapacity);
        builder.startArray("current_nodes");
        {
            for (DiscoveryNode node : currentNodes) {
                builder.startObject();
                builder.field("name", node.getName());
                builder.endObject();
            }
        }
        builder.endArray();
        builder.startObject("deciders");
        for (Map.Entry<String, AutoscalingDeciderResult> entry : results.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public AutoscalingCapacity requiredCapacity() {
        if (results.values().stream().map(AutoscalingDeciderResult::requiredCapacity).anyMatch(Objects::isNull)) {
            // any undetermined decider cancels out all required capacities
            return null;
        }
        Optional<AutoscalingCapacity> result = results.values()
            .stream()
            .map(AutoscalingDeciderResult::requiredCapacity)
            .reduce(AutoscalingCapacity::upperBound);
        assert result.isPresent();
        return result.get();
    }

    public AutoscalingCapacity currentCapacity() {
        return currentCapacity;
    }

    public SortedSet<DiscoveryNode> currentNodes() {
        return currentNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingDeciderResults that = (AutoscalingDeciderResults) o;
        return currentCapacity.equals(that.currentCapacity) && results.equals(that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentCapacity, results);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
