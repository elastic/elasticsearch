/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class SampleExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "SampleExec",
        SampleExec::new
    );

    private final Expression probability;
    private final Expression seed;

    public SampleExec(Source source, PhysicalPlan child, Expression probability, @Nullable Expression seed) {
        super(source, child);
        this.probability = probability;
        this.seed = seed;
    }

    public SampleExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class), // child
            in.readNamedWriteable(Expression.class), // probability
            in.readOptionalNamedWriteable(Expression.class) // seed
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(probability);
        out.writeOptionalNamedWriteable(seed);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new SampleExec(source(), newChild, probability, seed);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, SampleExec::new, child(), probability, seed);
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression probability() {
        return probability;
    }

    public Expression seed() {
        return seed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), probability, seed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        var other = (SampleExec) obj;

        return Objects.equals(child(), other.child()) && Objects.equals(probability, other.probability) && Objects.equals(seed, other.seed);
    }
}
