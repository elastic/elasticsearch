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
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Retains a ratio of series using hash sampling: each row is kept or dropped by hashing its
 * sampling key (the groupings without the step bucket), so the kept subset is stable however rows
 * are partitioned. The keep/drop decision is per-row stateless, unlike a count-based limit.
 * <p>
 * Unlike the other reductions ({@code TopNBy}) this is not a {@link PipelineBreaker}: the hash
 * filter is idempotent and needs no global per-group view, so it may run anywhere in the plan,
 * including pushed down into data-node fragments. In practice it sits above the aggregation that
 * produces its input (which breaks the pipeline itself), so it typically executes post-exchange
 * on the coordinator.
 */
public class LimitRatioBy extends UnaryPlan implements PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "LimitRatioBy",
        LimitRatioBy::new
    );

    private final Expression ratio;
    private final List<Expression> groupings;

    public LimitRatioBy(Source source, LogicalPlan child, Expression ratio, List<Expression> groupings) {
        super(source, child);
        this.ratio = ratio;
        this.groupings = groupings;
    }

    private LimitRatioBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ratio());
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LimitRatioBy> info() {
        return NodeInfo.create(this, LimitRatioBy::new, child(), ratio, groupings);
    }

    @Override
    public LimitRatioBy replaceChild(LogicalPlan newChild) {
        return new LimitRatioBy(source(), newChild, ratio, groupings);
    }

    public Expression ratio() {
        return ratio;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return ratio.resolved() && Resolvables.resolved(groupings);
    }

    /**
     * Validates the translator-built node with source context, so malformed plans fail here with a
     * clear error instead of deep in execution planning: the ratio must be a numeric literal and
     * every key carrier (the groupings without the leading step bucket) must be a resolved
     * attribute of the input.
     */
    @Override
    public void postOptimizationVerification(Failures failures) {
        Object folded = ratio.foldable() ? ratio.fold(FoldContext.small()) : null;
        if (folded instanceof Number number) {
            if (Double.isNaN(number.doubleValue())) {
                failures.add(fail(ratio, "LIMIT RATIO BY ratio must not be NaN"));
            }
        } else {
            failures.add(fail(ratio, "LIMIT RATIO BY ratio must be a numeric literal, got [{}]", folded));
        }
        for (Expression carrier : keyCarriers()) {
            if (carrier instanceof Attribute key) {
                if (child().output().stream().noneMatch(a -> a.id().equals(key.id()))) {
                    failures.add(fail(key, "LIMIT RATIO BY key [{}] is not produced by its input", key.name()));
                }
            } else {
                failures.add(fail(carrier, "LIMIT RATIO BY key expression must be an attribute"));
            }
        }
    }

    /**
     * The sampling key: the groupings without the leading step bucket (see
     * {@code TranslatePromqlToEsqlPlan#reductionGrouping}, which always places the step first).
     * The encoder accepts every element type, so no type check is needed.
     */
    private List<Expression> keyCarriers() {
        return groupings().subList(1, groupings().size());
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratio, child(), groupings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LimitRatioBy other = (LimitRatioBy) obj;
        return Objects.equals(ratio, other.ratio) && Objects.equals(child(), other.child()) && Objects.equals(groupings, other.groupings);
    }
}
