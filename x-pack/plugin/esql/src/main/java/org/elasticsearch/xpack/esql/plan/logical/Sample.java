/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplingQuery;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class Sample extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Sample", Sample::new);

    private final Expression probability;
    private final Expression seed;

    public Sample(Source source, Expression probability, @Nullable Expression seed, LogicalPlan child) {
        super(source, child);
        this.probability = probability;
        this.seed = seed;
    }

    private Sample(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class), // probability
            in.readOptionalNamedWriteable(Expression.class), // seed
            in.readNamedWriteable(LogicalPlan.class) // child
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(probability);
        out.writeOptionalNamedWriteable(seed);
        out.writeNamedWriteable(child());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Sample> info() {
        return NodeInfo.create(this, Sample::new, probability, seed, child());
    }

    @Override
    public Sample replaceChild(LogicalPlan newChild) {
        return new Sample(source(), probability, seed, newChild);
    }

    public Expression probability() {
        return probability;
    }

    public Expression seed() {
        return seed;
    }

    @Override
    public boolean expressionsResolved() {
        return probability.resolved() && (seed == null || seed.resolved());
    }

    @Override
    public int hashCode() {
        return Objects.hash(probability, seed, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        var other = (Sample) obj;

        return Objects.equals(probability, other.probability) && Objects.equals(seed, other.seed) && Objects.equals(child(), other.child());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        try {
            RandomSamplingQuery.checkProbabilityRange((double) Foldables.valueOf(FoldContext.small(), probability));
        } catch (IllegalArgumentException e) {
            failures.add(fail(probability, e.getMessage()));
        }
    }
}
