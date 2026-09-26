/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;

/**
 * Represents a PromQL aggregate function call that operates on range vectors.
 * <p>
 * These functions take a range vector as input and aggregate the values within each series
 * over the specified time range, returning an instant vector.
 * This corresponds to PromQL syntax:
 * <pre>
 * function_name(range_vector)
 * </pre>
 *
 * Examples:
 * <pre>
 * rate(http_requests_total[5m])
 * increase(errors_total[1h])
 * delta(cpu_temp_celsius[30m])
 * </pre>
 *
 * These functions operate independently on each time series selected by the range vector.
 * The result contains one sample per series at the evaluation timestamp.
 */
public final class WithinSeriesAggregate extends PromqlFunctionCall {

    private List<Attribute> output;

    public WithinSeriesAggregate(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, WithinSeriesAggregate::new, child(), definition(), parameters());
    }

    @Override
    public WithinSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new WithinSeriesAggregate(source(), newChild, definition(), parameters());
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            // absent_over_time is one series carrying the selector's equality matchers as labels; every other function
            // returns values grouped per time series
            output = isAbsentOverTime()
                ? absentLabels().keySet().stream().<Attribute>map(name -> new ReferenceAttribute(source(), name, DataType.KEYWORD)).toList()
                : List.of(FieldAttribute.timeSeriesAttribute(source()));
        }
        return output;
    }

    /**
     * {@code present_over_time} is {@code 1} for a series with a sample in the window and nothing otherwise, never {@code 0}.
     * {@code absent_over_time} is one {@code {labels} 1} row per step at which no series has a sample in the window, the
     * labels those of the selector's equality matchers, and nothing otherwise. Every other range function applies to its
     * series' values.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        if (isAbsentOverTime() && translation.cmd().start().value() != null) {
            return translateAbsent(translation);
        }
        if (isPresentOverTime() || isAbsentOverTime()) {
            // per series: the value 1 where the function holds, nothing where it does not (absent_over_time takes this
            // per-series form only over a query with no known range, which has no steps to synthesize a series for)
            List<String> name = List.of(LabelMatcher.NAME);
            TranslationResult child = translation.translate(child(), sub(translation.required(), of(name)));
            if (child.isEmpty()) {
                return child;
            }
            Expression holds = buildEsqlFunction(child.value(), translation.promqlContext(child, window(translation.cmd())));
            Expression one = new Case(source(), holds, List.of(Literal.fromDouble(source(), 1.0), Literal.NULL));
            return translation.eval(child, one).drop(name);
        }
        return super.translate(translation);
    }

    private TranslationResult translateAbsent(TranslationContext translation) {
        // IN: nothing - the result carries the matchers' labels, not the series'
        TranslationResult child = translation.translate(child(), of());
        TranslationResult present = child;
        if (child.isEmpty() == false) {
            // per series, whether the window holds a sample; per step, whether any series does (the value 1, or null)
            Expression absent = buildEsqlFunction(child.value(), translation.promqlContext(child, window(translation.cmd())));
            Expression one = new Case(source(), absent, List.of(Literal.NULL, Literal.fromDouble(source(), 1.0)));
            present = translation.aggregate(child, of(), new Max(source(), one));
        }
        // OUT: the equality matchers' labels
        return translation.absent(present, absentLabels());
    }

    private boolean isAbsentOverTime() {
        return functionName().equals(AbsentOverTime.PROMQL_DEFINITION.name());
    }

    private boolean isPresentOverTime() {
        return functionName().equals(PresentOverTime.PROMQL_DEFINITION.name());
    }

    /**
     * The labels of an absent series, as Prometheus derives them from the selector: each label with exactly one equality
     * matcher, the metric name aside; a label matched twice, or in any other way, is left out.
     */
    private Map<String, String> absentLabels() {
        var labels = new LinkedHashMap<String, String>();
        if (child() instanceof Selector selector) {
            var equal = new HashSet<String>();
            for (LabelMatcher matcher : selector.labelMatchers().matchers()) {
                if (LabelMatcher.NAME.equals(matcher.name())) {
                    continue;
                }
                if (matcher.matcher() == LabelMatcher.Matcher.EQ && matcher.isMultiValue() == false && equal.add(matcher.name())) {
                    labels.put(matcher.name(), matcher.getFirstValue());
                } else {
                    labels.remove(matcher.name());
                }
            }
        }
        return labels;
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.WITHIN_SERIES_AGGREGATION;
    }

    @Override
    public boolean dropsMetricName() {
        // last_over_time acts like an offset and keeps the metric name; every other range function drops it
        return functionName().equals(LastOverTime.PROMQL_DEFINITION.name()) == false;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Per-series aggregation (e.g. rate): series identity passes through unchanged.
        return true;
    }
}
