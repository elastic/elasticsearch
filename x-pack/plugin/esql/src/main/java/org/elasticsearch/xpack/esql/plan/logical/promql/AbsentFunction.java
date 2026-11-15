/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;

/**
 * PromQL {@code absent(v)}: a single series with the value {@code 1} at every evaluation step where {@code v} has no
 * element, and nothing at the other steps.
 * <p>
 * Steps only exist in a translated plan where documents exist, so a step without any element of {@code v} cannot be
 * produced from {@code v}'s own rows. The node therefore enumerates every evaluation step from the command's time range
 * and anti-joins it against the steps where {@code count(v)} has a value (see {@link #translate}).
 * <p>
 * The result's labels are derived from the equality matchers of a selector argument, as in Prometheus. They are minted
 * once as stable attributes and threaded unchanged through the node's lifecycle, so an enclosing {@code by(...)} that
 * resolved against them keeps referring to the same identities.
 */
public final class AbsentFunction extends PromqlFunctionCall {

    /** Prometheus metadata labels that are never copied onto the {@code absent} result. */
    private static final Set<String> METADATA_LABELS = Set.of(LabelMatcher.NAME, "__type__", "__unit__");

    private final List<Attribute> labels;
    private final List<String> labelValues;

    public AbsentFunction(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        this(source, child, definition, parameters, absentLabels(child));
    }

    private AbsentFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Map<String, String> derived
    ) {
        this(source, child, definition, parameters, mintLabels(source, derived), List.copyOf(derived.values()));
    }

    private AbsentFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        List<Attribute> labels,
        List<String> labelValues
    ) {
        super(source, child, definition, parameters);
        this.labels = labels;
        this.labelValues = labelValues;
    }

    /**
     * The labels Prometheus puts on the result ({@code createLabelsForAbsentFunction}), as label name to value in matcher
     * order. Only a selector argument has any: a label takes the value of its first equality matcher unless a later matcher
     * names the same label, and the metadata labels are skipped. An empty value counts as set but adds nothing, since an empty
     * label is no label.
     */
    static Map<String, String> absentLabels(LogicalPlan child) {
        if (child instanceof InstantSelector == false) {
            return Map.of();
        }
        InstantSelector selector = (InstantSelector) child;
        Map<String, String> set = new LinkedHashMap<>();
        Set<String> named = new HashSet<>();
        for (LabelMatcher matcher : selector.labelMatchers().matchers()) {
            String name = PromqlLabels.labelName(matcher.name());
            if (METADATA_LABELS.contains(name)) {
                continue;
            }
            boolean singleEquality = matcher.matcher() == LabelMatcher.Matcher.EQ && matcher.isMultiValue() == false;
            if (singleEquality && named.add(name)) {
                String value = matcher.getFirstValue();
                if (value.isEmpty() == false) {
                    set.put(name, value);
                }
            } else {
                set.remove(name);
            }
        }
        return set;
    }

    private static List<Attribute> mintLabels(Source source, Map<String, String> derived) {
        List<Attribute> minted = new ArrayList<>(derived.size());
        for (String name : derived.keySet()) {
            minted.add(new ReferenceAttribute(source, null, name, DataType.KEYWORD));
        }
        return List.copyOf(minted);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, AbsentFunction::new, child(), definition(), parameters(), labels, labelValues);
    }

    @Override
    public AbsentFunction replaceChild(LogicalPlan newChild) {
        return new AbsentFunction(source(), newChild, definition(), parameters(), labels, labelValues);
    }

    @Override
    public List<Attribute> output() {
        return labels;
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.METADATA_MANIPULATION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // The result is a new series whose identity is derived from the argument's matchers, not the argument's series.
        return false;
    }

    /**
     * Lowers {@code absent(v)} to {@code steps ANTI JOIN presence ON step}: the left side lists every evaluation step of the
     * command's range, the right side the steps where {@code count(v)} has a value. Every surviving step gets the value
     * {@code 1} and the derived labels as constants.
     * <p>
     * An argument that reads no source data outside {@code scalar()} has an element at every step (a literal or
     * {@code scalar()} result exists at every step, the latter as {@code NaN} when its input is empty), so the result is
     * empty. Translating it would instead read only the steps that have documents, so it is short-circuited to an empty
     * relation.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        PromqlCommand cmd = context.cmd();
        Source source = source();
        if (readsSource(child()) == false) {
            List<Attribute> columns = new ArrayList<>(List.of(cmd.valueAttribute(), cmd.stepAttribute()));
            columns.addAll(labels);
            var empty = new LocalRelation(cmd.source(), columns, EmptyLocalSupplier.EMPTY);
            return new IntermediateResult(
                empty,
                finite(TranslationContext.mapFinite(labels)),
                Literal.NULL,
                cmd.stepAttribute(),
                null,
                Kind.CONSTANT
            );
        }

        var count = new AcrossSeriesAggregate(
            source,
            child(),
            Count.PROMQL_DEFINITION,
            List.of(),
            AcrossSeriesAggregate.Grouping.NONE,
            List.of()
        );
        IntermediateResult presence = context.withRequired(TranslationSchema.EMPTY)
            .translateIntermediate(count, new NameId(), new NameId());
        Attribute presenceValue = presence.valueColumn();
        Attribute presenceStep = presence.step();
        // The anti join empties its whole result on any null key on the right side, so a step without a value must not reach it.
        LogicalPlan right = new Filter(
            source,
            presence.plan(),
            new And(source, new IsNotNull(source, presenceValue), new IsNotNull(source, presenceStep))
        );
        Alias presenceKey = new Alias(source, cmd.stepColumnName(), presenceStep);
        right = new Eval(source, right, List.of(presenceKey));
        right = new Project(source, right, List.of(presenceKey.toAttribute()));

        LogicalPlan steps = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        Attribute stepsKey = steps.output().getFirst();
        LogicalPlan plan = new AntiJoin(source, steps, right, List.of(stepsKey), List.of(presenceKey.toAttribute()));

        Alias value = new Alias(source, cmd.valueColumnName(), Literal.fromDouble(source, 1.0));
        Alias step = new Alias(source, cmd.stepColumnName(), stepsKey, context.stepAttr().id());
        List<Alias> definitions = new ArrayList<>(List.of(value, step));
        for (int i = 0; i < labels.size(); i++) {
            Attribute label = labels.get(i);
            definitions.add(new Alias(source, label.name(), Literal.keyword(source, labelValues.get(i)), label.id()));
        }
        plan = new Eval(cmd.source(), plan, definitions);
        List<NamedExpression> projections = new ArrayList<>(definitions.size());
        for (Alias definition : definitions) {
            projections.add(definition.toAttribute());
        }
        plan = new Project(cmd.source(), plan, projections);
        return new IntermediateResult(
            plan,
            finite(TranslationContext.mapFinite(labels)),
            value.toAttribute(),
            step.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    /**
     * Whether evaluating {@code plan} reads series from the source outside a {@code scalar()} subtree. A nested
     * {@code absent} counts as reading the source: its result can be empty at a step even when it reads nothing.
     */
    private static boolean readsSource(LogicalPlan plan) {
        if (plan instanceof AbsentFunction) {
            return true;
        }
        if (plan instanceof ScalarConversionFunction) {
            return false;
        }
        if (plan instanceof InstantSelector || plan instanceof RangeSelector) {
            return true;
        }
        return plan.children().stream().anyMatch(AbsentFunction::readsSource);
    }
}
