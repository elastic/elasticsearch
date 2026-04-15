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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
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
public class PromqlCommand extends UnaryPlan
    implements
        TelemetryAware,
        PostAnalysisVerificationAware,
        TimestampAware,
        TimestampBoundsAware.OfLogicalPlan {

    /**
     * The name of the column containing the step value (aka time bucket) in range queries.
     */
    private static final String STEP_COLUMN_NAME = "step";

    private final LogicalPlan promqlPlan;
    private final Literal start;
    private final Literal end;
    private final Literal step;
    private final Literal buckets;
    private final Literal scrapeInterval;
    // TODO: this should be made available through the planner
    private final Expression timestamp;
    private final String valueColumnName;
    private final NameId valueId;
    private final NameId stepId;
    private final boolean collapsed;
    private List<Attribute> output;

    // Range query constructor (collapsed=false)
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        Expression timestamp
    ) {
        this(
            source,
            child,
            promqlPlan,
            start,
            end,
            step,
            buckets,
            scrapeInterval,
            valueColumnName,
            new NameId(),
            new NameId(),
            timestamp,
            false
        );
    }

    // Range query constructor with collapsed flag
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        Expression timestamp,
        boolean collapsed
    ) {
        this(
            source,
            child,
            promqlPlan,
            start,
            end,
            step,
            buckets,
            scrapeInterval,
            valueColumnName,
            new NameId(),
            new NameId(),
            timestamp,
            collapsed
        );
    }

    // Range query constructor
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        NameId valueId,
        NameId stepId,
        Expression timestamp
    ) {
        this(source, child, promqlPlan, start, end, step, buckets, scrapeInterval, valueColumnName, valueId, stepId, timestamp, false);
    }

    // Full constructor with collapsed flag
    public PromqlCommand(
        Source source,
        LogicalPlan child,
        LogicalPlan promqlPlan,
        Literal start,
        Literal end,
        Literal step,
        Literal buckets,
        Literal scrapeInterval,
        String valueColumnName,
        NameId valueId,
        NameId stepId,
        Expression timestamp,
        boolean collapsed
    ) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.start = start;
        this.end = end;
        this.step = step;
        this.buckets = buckets;
        this.scrapeInterval = scrapeInterval;
        this.valueColumnName = valueColumnName;
        this.valueId = valueId;
        this.stepId = stepId;
        this.timestamp = timestamp;
        this.collapsed = collapsed;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(
            this,
            (s, child, plan, st, en, stp, bk, si, vcn, vi, sti, ts, col) -> new PromqlCommand(
                s,
                child,
                plan,
                st,
                en,
                stp,
                bk,
                si,
                vcn,
                vi,
                sti,
                ts,
                col
            ),
            child(),
            promqlPlan(),
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp(),
            isCollapsed()
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
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp(),
            collapsed
        );
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        if (newPromqlPlan == promqlPlan) {
            return this;
        }
        return new PromqlCommand(
            source(),
            child(),
            newPromqlPlan,
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp(),
            collapsed
        );
    }

    /**
     * Bounds are only needed when {@code buckets} is specified without an explicit time range.
     * When {@code step} alone is set, the query can proceed without start/end because the step
     * directly defines the bucket size; {@link #postAnalysisVerification} validates that case.
     */
    @Override
    public boolean needsTimestampBounds() {
        return buckets.value() != null && hasTimeRange() == false;
    }

    @Override
    public PromqlCommand withTimestampBounds(Literal start, Literal end) {
        return new PromqlCommand(
            source(),
            child(),
            promqlPlan(),
            start,
            end,
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp(),
            collapsed
        );
    }

    public PromqlCommand withCollapsed(boolean collapsed) {
        return new PromqlCommand(
            source(),
            child(),
            promqlPlan(),
            start(),
            end(),
            step(),
            buckets(),
            scrapeInterval(),
            valueColumnName(),
            valueId(),
            stepId(),
            timestamp(),
            collapsed
        );
    }

    public boolean isCollapsed() {
        return collapsed;
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

    /**
     * Number of buckets for auto-derived range-query bucket size.
     */
    public Literal buckets() {
        return buckets;
    }

    /**
     * The expected scrape interval used to derive implicit range selector windows.
     */
    public Literal scrapeInterval() {
        return scrapeInterval;
    }

    public boolean hasTimeRange() {
        return start.value() != null && end.value() != null;
    }

    public boolean isInstantQuery() {
        return step.value() == null && buckets.value() == null;
    }

    public boolean isRangeQuery() {
        return isInstantQuery() == false;
    }

    public String valueColumnName() {
        return valueColumnName;
    }

    public String stepColumnName() {
        return STEP_COLUMN_NAME;
    }

    public NameId valueId() {
        return valueId;
    }

    public NameId stepId() {
        return stepId;
    }

    public ReferenceAttribute valueAttribute() {
        return new ReferenceAttribute(source(), null, valueColumnName, DataType.DOUBLE, Nullability.FALSE, valueId, false);
    }

    public ReferenceAttribute stepAttribute() {
        return new ReferenceAttribute(source(), null, stepColumnName(), DataType.DATETIME, Nullability.FALSE, stepId, false);
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
            output.add(valueAttribute());
            output.add(stepAttribute());
            output.addAll(additionalOutput);
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            child(),
            promqlPlan,
            start,
            end,
            step,
            buckets,
            scrapeInterval,
            valueColumnName,
            valueId,
            stepId,
            timestamp,
            collapsed
        );
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
                && Objects.equals(buckets, other.buckets)
                && Objects.equals(scrapeInterval, other.scrapeInterval)
                && Objects.equals(valueColumnName, other.valueColumnName)
                && Objects.equals(valueId, other.valueId)
                && Objects.equals(stepId, other.stepId)
                && Objects.equals(timestamp, other.timestamp)
                && collapsed == other.collapsed;
        }

        return false;
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format) {
        sb.append(nodeName());
        sb.append(" start=[").append(start);
        sb.append("] end=[").append(end);
        sb.append("] step=[").append(step);
        sb.append("] buckets=[").append(buckets);
        sb.append("] scrape_interval=[").append(scrapeInterval);
        sb.append("] valueColumnName=[").append(valueColumnName);
        sb.append("] promql=[<>\n");
        sb.append(promqlPlan.toString());
        sb.append("\n<>]]");
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
        boolean hasStep = step.value() != null;
        boolean hasRangeAndBuckets = start.value() != null && end.value() != null && buckets.value() != null;
        if (hasStep == false && hasRangeAndBuckets == false) {
            failures.add(
                fail(
                    this,
                    "unable to create a bucket; provide either [{}] or all of [{}], [{}], and [{}] [{}]",
                    "step",
                    "start",
                    "end",
                    "buckets",
                    sourceText()
                )
            );
            return;
        }
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
                    } else if (s.series() instanceof FieldAttribute seriesField) {
                        if (seriesField.isDimension()) {
                            failures.add(
                                fail(
                                    s,
                                    "field [{}] of type [{}] cannot be used as a metric; it is a dimension field [{}]",
                                    seriesField.name(),
                                    seriesField.dataType().typeName(),
                                    s.sourceText()
                                )
                            );
                        }
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
                case AcrossSeriesAggregate agg -> {
                    if (agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT && usesWithoutGrouping(agg.child())) {
                        failures.add(fail(agg, "nested WITHOUT over WITHOUT is not supported at this time [{}]", agg.sourceText()));
                    }
                    // Reject labels whose name collides with the built-in step column.
                    // If this proves too restrictive, we could add an option to rename the built-in step column.
                    for (Attribute grouping : agg.groupings()) {
                        if (stepColumnName().equals(grouping.name())) {
                            failures.add(
                                fail(
                                    agg,
                                    "label [{}] collides with the built-in [{}] output column [{}]",
                                    stepColumnName(),
                                    stepColumnName(),
                                    agg.sourceText()
                                )
                            );
                        }
                    }
                }
                case PromqlFunctionCall functionCall -> {
                    validateCounterSupport(functionCall, failures);
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
                    if (usesWithoutGrouping(binaryOperator.left()) || usesWithoutGrouping(binaryOperator.right())) {
                        failures.add(fail(lp, "binary expressions with WITHOUT are not supported at this time [{}]", lp.sourceText()));
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

    private static boolean usesWithoutGrouping(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT);
    }

    /**
     * Validates that the metric field type is compatible with the function's counter support.
     * Only checks when the function's direct child is a RangeSelector, because InstantSelectors
     * are implicitly wrapped in LastOverTime during translation, which converts counter types
     * to their numeric base types. RangeSelectors pass the raw field type through to the function.
     */
    private static void validateCounterSupport(PromqlFunctionCall functionCall, Failures failures) {
        if (functionCall.child() instanceof RangeSelector s && s.series() instanceof FieldAttribute seriesField) {
            DataType seriesType = seriesField.dataType();
            if (DataType.isNull(seriesType)) {
                return;
            }
            var metadata = PromqlFunctionRegistry.INSTANCE.functionMetadata(functionCall.functionName());
            if (metadata == null) {
                return;
            }
            var counterSupport = metadata.counterSupport();
            if (DataType.isCounter(seriesType) && counterSupport == PromqlFunctionRegistry.CounterSupport.UNSUPPORTED) {
                failures.add(
                    fail(
                        functionCall,
                        "function [{}] does not support counter metric [{}] of type [{}];"
                            + " use rate() or increase() to convert counters first [{}]",
                        functionCall.functionName(),
                        seriesField.name(),
                        seriesType.typeName(),
                        functionCall.sourceText()
                    )
                );
            } else if (DataType.isCounter(seriesType) == false && counterSupport == PromqlFunctionRegistry.CounterSupport.REQUIRED) {
                failures.add(
                    fail(
                        functionCall,
                        "function [{}] requires a counter metric, but [{}] has type [{}] [{}]",
                        functionCall.functionName(),
                        seriesField.name(),
                        seriesType.typeName(),
                        functionCall.sourceText()
                    )
                );
            }
        }
    }
}
