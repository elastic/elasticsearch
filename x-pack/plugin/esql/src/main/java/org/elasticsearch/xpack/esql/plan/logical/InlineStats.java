/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Enriches the stream of data with the results of running a {@link Aggregate STATS}.
 * <p>
 *     Maps to a dedicated Join implementation, InlineJoin, which is a left join between the main relation and the
 *     underlying aggregate.
 * </p>
 */
public class InlineStats extends UnaryPlan
    implements
        NamedWriteable,
        SurrogateLogicalPlan,
        TelemetryAware,
        SortAgnostic,
        PostAnalysisPlanVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "InlineStats",
        InlineStats::new
    );

    private final Aggregate aggregate;
    private List<Attribute> lazyOutput;

    public InlineStats(Source source, Aggregate aggregate) {
        super(source, aggregate);
        this.aggregate = aggregate;
    }

    public InlineStats(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), (Aggregate) in.readNamedWriteable(LogicalPlan.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(aggregate);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<InlineStats> info() {
        return NodeInfo.create(this, InlineStats::new, aggregate);
    }

    @Override
    public InlineStats replaceChild(LogicalPlan newChild) {
        return new InlineStats(source(), (Aggregate) newChild);
    }

    public Aggregate aggregate() {
        return aggregate;
    }

    @Override
    public boolean expressionsResolved() {
        return aggregate.expressionsResolved();
    }

    @Override
    public List<Attribute> output() {
        if (this.lazyOutput == null) {
            this.lazyOutput = mergeOutputAttributes(aggregate.output(), aggregate.child().output());
        }
        return lazyOutput;
    }

    // TODO: in case of INLINE STATS, the join key is always the grouping
    private JoinConfig joinConfig() {
        List<Expression> groupings = aggregate.groupings();
        List<Attribute> namedGroupings = new ArrayList<>(groupings.size());
        for (Expression g : groupings) {
            namedGroupings.add(Expressions.attribute(g));
        }
        // last named grouping wins, just like it happens for regular STATS
        // ie BY x = field_1, x = field_2, the grouping is actually performed on second x (field_2)
        namedGroupings = mergeOutputAttributes(namedGroupings, emptyList());

        List<Attribute> leftFields = new ArrayList<>(groupings.size());
        List<Attribute> rightFields = new ArrayList<>(groupings.size());
        List<Attribute> rhsOutput = Join.makeReference(aggregate.output());
        for (Attribute lhs : namedGroupings) {
            for (Attribute rhs : rhsOutput) {
                if (lhs.name().equals(rhs.name())) {
                    leftFields.add(lhs);
                    rightFields.add(rhs);
                    break;
                }
            }
        }
        return new JoinConfig(JoinTypes.LEFT, leftFields, rightFields, null);
    }

    @Override
    public LogicalPlan surrogate() {
        // left join between the main relation and the local, lookup relation
        Source source = source();
        LogicalPlan left = aggregate.child();
        return new InlineJoin(source, left, InlineJoin.stubSource(aggregate, left), joinConfig());
    }

    @Override
    public String telemetryLabel() {
        return "INLINE STATS";
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregate, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InlineStats other = (InlineStats) obj;
        return Objects.equals(aggregate, other.aggregate);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (p, failures) -> {
            // Allow inline stats to be used with TS command if it follows a STATS command
            // Examples:
            // valid: TS metrics | STATS ...
            // valid: TS metrics | STATS ... | INLINE STATS ...
            // invalid: TS metrics | INLINE STATS ...
            // invalid: TS metrics | INLINE STATS ... | STATS ...
            if (p instanceof InlineStats inlineStats) {
                Holder<Boolean> foundInlineStats = new Holder<>(false);
                Holder<Boolean> foundPreviousStats = new Holder<>(false);
                Holder<Boolean> isTimeSeries = new Holder<>(false);
                inlineStats.child().forEachUp(lp -> {
                    if (lp instanceof Aggregate) {
                        if (foundInlineStats.get() == false) {
                            foundInlineStats.set(true);
                        } else {
                            foundPreviousStats.set(true);
                        }
                    } else if (lp instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES) {
                        isTimeSeries.set(true);
                    }
                });
                if (isTimeSeries.get() && foundPreviousStats.get() == false) {
                    failures.add(
                        fail(inlineStats, "INLINE STATS [{}] can only be used after STATS when used with TS command", this.sourceText())
                    );
                }
            }
        };
    }
}
