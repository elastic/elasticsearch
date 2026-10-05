/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.NaturalSortKey;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * PromQL {@code sort_by_label} / {@code sort_by_label_desc}: identity-preserving ordering of an instant vector
 * by one or more label values. Requested labels that resolve on an open header (the child still exposes
 * {@code _timeseries}) are added to the command output so translation can materialize them, and are projected
 * away again above the sort so ordering does not widen the result schema. On a closed header they are skipped
 * so the sort cannot split already-aggregated series. Ordering is injected above {@code TimeSeriesCollapse}
 * by the result-ordering analyzer rule.
 */
public final class SortByLabelFunction extends PromqlFunctionCall implements ResultOrderingFunction {

    private final List<Attribute> sortLabels;
    /** Both derive from the fixed child output and {@link #sortLabels}, and {@code output()} is read repeatedly. */
    private List<Attribute> usableSortLabels;
    private List<Attribute> output;

    public SortByLabelFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        List<Attribute> sortLabels
    ) {
        super(source, child, definition, parameters);
        this.sortLabels = sortLabels;
    }

    public List<Attribute> sortLabels() {
        return sortLabels;
    }

    /**
     * The sort labels exposed as extra output columns so an ordering can bind to them: resolved, non-null, a
     * dimension (the only fields that are series labels), and not already carried by the child. Only an open child
     * header ({@code _timeseries} still present) can carry them; on a closed one they are all dropped, because
     * materializing a label the aggregation merged away would split its series.
     */
    public List<Attribute> usableSortLabels() {
        if (usableSortLabels == null) {
            usableSortLabels = computeUsableSortLabels();
        }
        return usableSortLabels;
    }

    private List<Attribute> computeUsableSortLabels() {
        List<Attribute> childOut = child().output();
        boolean open = false;
        Set<String> childKeys = new HashSet<>();
        for (Attribute attribute : childOut) {
            childKeys.add(PromqlLabels.labelName(attribute));
            if (MetadataAttribute.TIMESERIES.equals(attribute.name())) {
                open = true;
            }
        }
        if (open == false) {
            return List.of();
        }
        List<Attribute> usable = new ArrayList<>();
        for (Attribute label : sortLabels) {
            if (label.resolved() == false || label.dataType() == DataType.NULL) {
                continue;
            }
            if (label instanceof FieldAttribute field && field.isDimension() == false) {
                continue;
            }
            String key = PromqlLabels.labelName(label);
            if (childKeys.add(key) == false) {
                continue;
            }
            usable.add(label);
        }
        return usable;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(sortLabels) && super.expressionsResolved();
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, SortByLabelFunction::new, child(), definition(), parameters(), sortLabels);
    }

    @Override
    public SortByLabelFunction replaceChild(LogicalPlan newChild) {
        return new SortByLabelFunction(source(), newChild, definition(), parameters(), sortLabels);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            List<Attribute> childOut = child().output();
            List<Attribute> extra = usableSortLabels();
            if (extra.isEmpty()) {
                output = childOut;
            } else {
                List<Attribute> out = new ArrayList<>(childOut.size() + extra.size());
                out.addAll(childOut);
                out.addAll(extra);
                output = out;
            }
        }
        return output;
    }

    @Override
    public List<Attribute> orderingOnlyColumns() {
        return usableSortLabels();
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.RESULT_ORDERING;
    }

    @Override
    public boolean isIdentityTransparent() {
        return true;
    }

    @Override
    public ResultOrdering resultOrdering(List<Attribute> commandOutput, Configuration configuration) {
        boolean desc = definition().name().equals("sort_by_label_desc");
        Order.OrderDirection direction = desc ? Order.OrderDirection.DESC : Order.OrderDirection.ASC;
        // A requested label that is absent compares as the empty string, which precedes every other value; the requested
        // keys coalesce to "" and are never null. A tie-break column is null where its label is absent. Ordering nulls
        // first ascending - and last descending - matches the Prometheus full-label-set comparison only when the series
        // lacking the label has no label sorting after it; otherwise Prometheus places that series after the other.
        Order.NullsPosition nulls = desc ? Order.NullsPosition.LAST : Order.NullsPosition.FIRST;
        List<Alias> syntheticKeys = new ArrayList<>();
        List<Order> orders = new ArrayList<>();
        Set<String> usedLabels = new HashSet<>();
        for (Attribute requested : sortLabels) {
            String labelName = PromqlLabels.labelName(requested);
            if (usedLabels.contains(labelName)) {
                continue;
            }
            Attribute inOutput = findIdentityAttribute(commandOutput, labelName);
            if (inOutput == null) {
                continue;
            }
            usedLabels.add(labelName);
            Alias key = naturalSortKey(inOutput, configuration);
            syntheticKeys.add(key);
            orders.add(new Order(source(), key.toAttribute(), direction, nulls));
        }
        Attribute timeseries = findIdentityAttribute(commandOutput, MetadataAttribute.TIMESERIES);
        if (timeseries != null) {
            orders.add(new Order(source(), timeseries, direction, nulls));
        } else {
            // Prometheus breaks ties by comparing the full label set, which it keeps sorted by label name.
            List<Attribute> remaining = new ArrayList<>();
            for (int i = 2; i < commandOutput.size(); i++) {
                Attribute attr = commandOutput.get(i);
                if (usedLabels.contains(PromqlLabels.labelName(attr)) == false) {
                    remaining.add(attr);
                }
            }
            remaining.sort(Comparator.comparing(PromqlLabels::labelName));
            for (Attribute attr : remaining) {
                orders.add(new Order(source(), attr, direction, nulls));
            }
        }
        return new ResultOrdering(List.copyOf(syntheticKeys), List.copyOf(orders));
    }

    /**
     * Encodes a label so unsigned byte order of the key is natsort order. {@code COALESCE(ToString(label), "")}
     * makes a missing value compare as the empty string, matching Prometheus.
     */
    private Alias naturalSortKey(Attribute label, Configuration configuration) {
        String keyName = Attribute.rawTemporaryName("promql_sort", PromqlLabels.labelName(label));
        Expression asString = new ToString(source(), label, configuration);
        Expression coalesced = new Coalesce(source(), asString, List.of(Literal.keyword(source(), "")));
        return new Alias(source(), keyName, new NaturalSortKey(source(), coalesced), null, true);
    }

    /**
     * Looks up a command-output identity column by PromQL label name, skipping {@code value} and {@code step}
     * ({@code commandOutput[0]} and {@code [1]}).
     */
    private static Attribute findIdentityAttribute(List<Attribute> commandOutput, String labelName) {
        for (int i = 2; i < commandOutput.size(); i++) {
            Attribute attr = commandOutput.get(i);
            if (labelName.equals(PromqlLabels.labelName(attr))) {
                return attr;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            SortByLabelFunction that = (SortByLabelFunction) o;
            return Objects.equals(sortLabels, that.sortLabels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortLabels);
    }
}
