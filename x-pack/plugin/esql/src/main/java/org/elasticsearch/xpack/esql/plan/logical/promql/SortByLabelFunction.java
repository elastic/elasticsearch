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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * PromQL {@code sort_by_label} / {@code sort_by_label_desc}: identity-preserving ordering of an instant vector
 * by one or more label values. Requested labels that resolve on an open header (the child still exposes
 * {@code _timeseries}) are added to the command output so translation can materialize them; on a closed header
 * they are skipped so the sort cannot split already-aggregated series. Ordering is injected above
 * {@code TimeSeriesCollapse} by the result-ordering analyzer rule.
 */
public final class SortByLabelFunction extends PromqlFunctionCall implements ResultOrderingFunction {

    private final List<Attribute> sortLabels;

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
     * Sort labels that can be materialized as extra output columns: resolved, non-null, not a metric field,
     * not already in the child output, and only when the child header is still open ({@code _timeseries}
     * is present). Absent labels are skipped; Prometheus treats those as {@code ""} at compare time.
     */
    public List<Attribute> usableSortLabels() {
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
            if (label instanceof FieldAttribute field && field.isMetric()) {
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
        List<Attribute> childOut = child().output();
        List<Attribute> extra = usableSortLabels();
        if (extra.isEmpty()) {
            return childOut;
        }
        List<Attribute> out = new ArrayList<>(childOut.size() + extra.size());
        out.addAll(childOut);
        out.addAll(extra);
        return out;
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
            orders.add(new Order(source(), key.toAttribute(), direction, Order.NullsPosition.LAST));
        }
        Attribute timeseries = findIdentityAttribute(commandOutput, MetadataAttribute.TIMESERIES);
        if (timeseries != null) {
            orders.add(new Order(source(), timeseries, direction, Order.NullsPosition.LAST));
        } else {
            for (int i = 2; i < commandOutput.size(); i++) {
                Attribute attr = commandOutput.get(i);
                if (usedLabels.contains(PromqlLabels.labelName(attr))) {
                    continue;
                }
                orders.add(new Order(source(), attr, direction, Order.NullsPosition.LAST));
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
