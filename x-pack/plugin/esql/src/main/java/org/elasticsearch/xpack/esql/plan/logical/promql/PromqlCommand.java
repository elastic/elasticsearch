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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.subquery.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware {

    private final LogicalPlan promqlPlan;
    private final PromqlParams params;

    // Range query constructor
    public PromqlCommand(Source source, LogicalPlan child, LogicalPlan promqlPlan, PromqlParams params) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.params = params;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(this, PromqlCommand::new, child(), promqlPlan(), params());
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(source(), newChild, promqlPlan(), params());
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        return new PromqlCommand(source(), child(), newPromqlPlan, params());
    }

    @Override
    public boolean expressionsResolved() {
        return promqlPlan.resolved();
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

    public PromqlParams params() {
        return params;
    }

    public Instant start() {
        return params().start();
    }

    public Instant end() {
        return params().end();
    }

    public Instant time() {
        return params().time();
    }

    public Duration step() {
        return params().step();
    }

    public boolean isInstantQuery() {
        return params().isInstantQuery();
    }

    public boolean isRangeQuery() {
        return params().isRangeQuery();
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), params, promqlPlan);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {

            PromqlCommand other = (PromqlCommand) obj;
            return Objects.equals(child(), other.child()) && Objects.equals(promqlPlan, other.promqlPlan);
        }

        return false;
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("params=");
        sb.append(params.toString());
        sb.append(" promql=[<>\n");
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
                if (s.evaluation() != null && s.evaluation().offset() != null && s.evaluation().offset().isZero() == false) {
                    failures.add(fail(s, "offset modifiers are not supported at this time [{}]", s.sourceText()));
                }
            }
            if (step() != null && lp instanceof RangeSelector rs) {
                Duration rangeDuration = (Duration) rs.range().fold(null);
                if (rangeDuration.equals(step()) == false) {
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
}
