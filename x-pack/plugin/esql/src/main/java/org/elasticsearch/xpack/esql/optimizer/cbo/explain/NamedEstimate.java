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
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;

import java.io.IOException;
import java.util.Objects;

/**
 * A statistic that a decision's cost depended on, such as the row count of a join side.
 * {@link DecisionReport} renders a list of these as one object keyed by {@link #name()}, in list order.
 */
public record NamedEstimate(String name, EstimateView estimate) implements Writeable {

    public NamedEstimate {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(estimate, "estimate");
    }

    public NamedEstimate(StreamInput in) throws IOException {
        this(in.readString(), new EstimateView(in));
    }

    public static NamedEstimate of(String name, Estimate e) {
        return new NamedEstimate(name, EstimateView.of(e));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        estimate.writeTo(out);
    }
}
