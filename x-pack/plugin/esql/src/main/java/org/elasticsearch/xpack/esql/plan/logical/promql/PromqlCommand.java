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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware, TimestampAware {

    /**
     * The name of the column containing the step value (aka time bucket) in range queries.
     */
    private static final String STEP_COLUMN_NAME = "step";

    private final LogicalPlan promqlPlan;
    private final Literal start;
    private final Literal end;
    private final Literal step;
    // TODO: this should be made available through the planner
    private final Expression timestamp;
    private final String valueColumnName;
    private final NameId valueId;
    private final NameId stepId;
    private List<Attribute> output;

    // Range query constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        String valueColumnName,
        Expression timestamp
    ) {
        this(source, child, promqlPlan, start, end, step, valueColumnName, new NameId(), new NameId(), timestamp);
    }

    // Range query constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        String valueColumnName,
        NameId valueId,
        NameId stepId,
        Expression timestamp
    ) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.start = start;
        this.end = end;
        this.step = step;
        this.valueColumnName = valueColumnName;
        this.valueId = valueId;
        this.stepId = stepId;
        this.timestamp = timestamp;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(
            this,
            PromqlCommand::new,
            child(),
            promqlPlan(),
            start(),
            end(),
            step(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(
            source(),
            newChild,
            promqlPlan(),
            start(),
            end(),
            step(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        return new PromqlCommand(
            source(),
            child(),
            newPromqlPlan,
            start(),
            end(),
            step(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp()
        );
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

    public String valueColumnName() {
        return valueColumnName;
    }

    public NameId valueId() {
        return valueId;
    }

    public NameId stepId() {
        return stepId;
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            List<Attribute> additionalOutput = promqlPlan.output();
            output = new ArrayList<>(additionalOutput.size() + 2);
            output.add(new ReferenceAttribute(source(), null, valueColumnName, DataType.DOUBLE, Nullability.FALSE, valueId, false));
            output.add(new ReferenceAttribute(source(), null, STEP_COLUMN_NAME, DataType.DATETIME, Nullability.FALSE, stepId, false));
            output.addAll(additionalOutput);
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), promqlPlan, start, end, step, valueColumnName, valueId, stepId, timestamp);
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
                && Objects.equals(valueColumnName, other.valueColumnName)
                && Objects.equals(valueId, other.valueId)
                && Objects.equals(stepId, other.stepId)
                && Objects.equals(timestamp, other.timestamp);
        }

        return false;
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append(" start=[").append(start);
        sb.append("] end=[").append(end);
        sb.append("] step=[").append(step);
        sb.append("] valueColumnName=[").append(valueColumnName);
        sb.append("] promql=[<>\n");
        sb.append(promqlPlan.toString());
        sb.append("\n<>]]");
        return sb.toString();
    }

    @Override
    protected AttributeSet computeReferences() {
        // Ensures field resolution is aware of all attributes used in the PromQL plan.
        AttributeSet.Builder references = AttributeSet.builder();
        promqlPlan().forEachDown(lp -> references.addAll(lp.references()));
        return references.build();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        LogicalPlan p = promqlPlan();
        // TODO(sidosera): Remove once instant query support is added.
        if (isInstantQuery()) {
            failures.add(fail(p, "instant queries are not supported at this time [{}]", sourceText()));
            return;
        }

        if (p instanceof RangeSelector && isRangeQuery()) {
            failures.add(
                fail(p, "invalid expression type \"range vector\" for range query, must be scalar or instant vector", p.sourceText())
            );
        }

        // Validate entire plan
        Holder<Boolean> root = new Holder<>(true);
        p.forEachDown(lp -> {
            switch (lp) {
                case Selector s -> {
                    if (s.labelMatchers().nameLabel() != null && s.labelMatchers().nameLabel().matcher().isRegex()) {
                        failures.add(fail(s, "regex label selectors on __name__ are not supported at this time [{}]", s.sourceText()));
                    }
                    if (s.series() == null) {
                        failures.add(fail(s, "__name__ label selector is required at this time [{}]", s.sourceText()));
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
                case PromqlFunctionCall functionCall -> {
                    if (functionCall instanceof AcrossSeriesAggregate asa) {
                        if (asa.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT) {
                            failures.add(fail(asa, "'without' grouping is not supported at this time [{}]", asa.sourceText()));
                        }
                    }
                }
                case ScalarFunction scalarFunction -> {
                    // ok
                }
                case VectorBinaryOperator binaryOperator -> {
                    binaryOperator.children().forEach(child -> {
                        if (child instanceof RangeSelector) {
                            failures.add(
                                fail(child, "binary expression must contain only scalar and instant vector types", child.sourceText())
                            );
                        }
                    });
                    if (binaryOperator.match() != VectorMatch.NONE) {
                        failures.add(
                            fail(
                                lp,
                                "{} queries with group modifiers are not supported at this time [{}]",
                                lp.getClass().getSimpleName(),
                                lp.sourceText()
                            )
                        );
                    }
                    if (binaryOperator instanceof VectorBinaryComparison comp) {
                        if (root.get() == false) {
                            failures.add(
                                fail(lp, "comparison operators are only supported at the top-level at this time [{}]", lp.sourceText())
                            );
                        }
                        if (comp.right() instanceof LiteralSelector == false) {
                            failures.add(
                                fail(
                                    lp,
                                    "comparison operators with non-literal right-hand side are not supported at this time [{}]",
                                    lp.sourceText()
                                )
                            );
                        }
                    }
                    if (binaryOperator instanceof VectorBinarySet) {
                        failures.add(fail(lp, "set operators are not supported at this time [{}]", lp.sourceText()));
                    }
                }
                case PlaceholderRelation placeholderRelation -> {
                    // ok
                }
                default -> failures.add(
                    fail(lp, "{} queries are not supported at this time [{}]", lp.getClass().getSimpleName(), lp.sourceText())
                );
            }
            root.set(false);
        });
    }
}
