/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.promql.subquery.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware, TimestampAware {

    private final LogicalPlan promqlPlan;
    private final Literal start;
    private final Literal end;
    private final Literal step;
    // TODO: this should be made available through the planner
    private final Expression timestamp;

    // Range query constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Expression timestamp
    ) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.start = start;
        this.end = end;
        this.step = step;
        this.timestamp = timestamp;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(this, PromqlCommand::new, child(), promqlPlan(), start(), end(), step(), timestamp());
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(source(), newChild, promqlPlan(), start(), end(), step(), timestamp());
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        return new PromqlCommand(source(), child(), newPromqlPlan, start(), end(), step(), timestamp());
    }

    @Override
    public boolean expressionsResolved() {
        return promqlPlan.resolved() && timestamp.resolved();
    }

    @Override
    public String telemetryLabel() {
        return "PROMQL";
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("serialization not supported");
    }

    public LogicalPlan promqlPlan() {
        return promqlPlan;
    }

    public Literal start() {
        return start;
    }

    public Literal end() {
        return end;
    }

    public Literal step() {
        return step;
    }

    public boolean isInstantQuery() {
        return step.value() == null;
    }

    public boolean isRangeQuery() {
        return step.value() != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), promqlPlan, start, end, step, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {

            PromqlCommand other = (PromqlCommand) obj;
            return Objects.equals(child(), other.child())
                && Objects.equals(promqlPlan, other.promqlPlan)
                && Objects.equals(start, other.start)
                && Objects.equals(end, other.end)
                && Objects.equals(step, other.step)
                && Objects.equals(timestamp, other.timestamp);
        }

        return false;
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append(" start=[").append(start);
        sb.append("] end=[").append(end);
        sb.append("] step=[").append(step);
        sb.append("] promql=[<>\n");
        sb.append(promqlPlan.toString());
        sb.append("\n<>]]");
        return sb.toString();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        LogicalPlan p = promqlPlan();
        if (p instanceof AcrossSeriesAggregate == false) {
            failures.add(fail(p, "only aggregations across timeseries are supported at this time (found [{}])", p.sourceText()));
        }
        p.forEachDown(lp -> {
            if (lp instanceof Selector s) {
                if (s.labelMatchers().nameLabel().matcher().isRegex()) {
                    failures.add(fail(s, "regex label selectors on __name__ are not supported at this time [{}]", s.sourceText()));
                }
                if (s.evaluation() != null) {
                    if (s.evaluation().offset().value() != null && s.evaluation().offsetDuration().isZero() == false) {
                        failures.add(fail(s, "offset modifiers are not supported at this time [{}]", s.sourceText()));
                    }
                    if (s.evaluation().at().value() != null) {
                        failures.add(fail(s, "@ modifiers are not supported at this time [{}]", s.sourceText()));
                    }
                }
            }
            if (lp instanceof Subquery) {
                failures.add(fail(lp, "subqueries are not supported at this time [{}]", lp.sourceText()));
            }
            if (step().value() != null && lp instanceof RangeSelector rs) {
                Duration rangeDuration = (Duration) rs.range().fold(null);
                if (rangeDuration.equals(step().value()) == false) {
                    failures.add(
                        fail(
                            rs.range(),
                            "the duration for range vector selector [{}] "
                                + "must be equal to the query's step for range queries at this time",
                            rs.range().sourceText()
                        )
                    );
                }
            }
        });
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
