/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class RoutingInfoUpdate implements Writeable {

    private final Optional<Integer> numberOfAllocations;
    private final Optional<RoutingStateAndReason> stateAndReason;

    public static RoutingInfoUpdate updateNumberOfAllocations(int numberOfAllocations) {
        return new RoutingInfoUpdate(Optional.of(numberOfAllocations), Optional.empty());
    }

    public static RoutingInfoUpdate updateStateAndReason(RoutingStateAndReason routingStateAndReason) {
        return new RoutingInfoUpdate(Optional.empty(), Optional.of(routingStateAndReason));
    }

    private RoutingInfoUpdate(Optional<Integer> numberOfAllocations, Optional<RoutingStateAndReason> stateAndReason) {
        this.numberOfAllocations = Objects.requireNonNull(numberOfAllocations);
        this.stateAndReason = Objects.requireNonNull(stateAndReason);
    }

    public RoutingInfoUpdate(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            numberOfAllocations = Optional.ofNullable(in.readOptionalVInt());
            stateAndReason = Optional.ofNullable(in.readOptionalWriteable(RoutingStateAndReason::new));
        } else {
            numberOfAllocations = Optional.empty();
            stateAndReason = Optional.of(new RoutingStateAndReason(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            out.writeOptionalVInt(numberOfAllocations.orElse(null));
            out.writeOptionalWriteable(stateAndReason.orElse(null));
        } else {
            assert stateAndReason.isPresent() : "updating routing info while nodes prior to 8.4.0 should only contain state and reason";
            stateAndReason.get().writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoutingInfoUpdate that = (RoutingInfoUpdate) o;
        return Objects.equals(numberOfAllocations, that.numberOfAllocations) && Objects.equals(stateAndReason, that.stateAndReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfAllocations, stateAndReason);
    }

    @Override
    public String toString() {
        return "RoutingInfoUpdate{" + "numberOfAllocations=" + numberOfAllocations + ", stateAndReason=" + stateAndReason + '}';
    }

    public Optional<Integer> getNumberOfAllocations() {
        return numberOfAllocations;
    }

    public Optional<RoutingStateAndReason> getStateAndReason() {
        return stateAndReason;
    }

    public RoutingInfo apply(RoutingInfo routingInfo) {
        int currentAllocations = numberOfAllocations.orElse(routingInfo.getCurrentAllocations());
        RoutingState state = routingInfo.getState();
        String reason = routingInfo.getReason();
        if (stateAndReason.isPresent()) {
            state = stateAndReason.get().getState();
            reason = stateAndReason.get().getReason();
        }
        return new RoutingInfo(currentAllocations, routingInfo.getTargetAllocations(), state, reason);
    }
}
