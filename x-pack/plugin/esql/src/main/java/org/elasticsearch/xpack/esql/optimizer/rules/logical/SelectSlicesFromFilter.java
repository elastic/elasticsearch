/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Records on a source the slices that the filter sitting on it restricts it to. The filter stays in the plan: the slices
 * only tell the engine which shards to query and which slices each search context reads.
 * <p>
 * Slices are read from the conditions and-ed at the top level of the filter that have the form {@code _slice == <literal>}
 * or {@code _slice IN (<literals>)}, or are a disjunction of those. The rule runs once the filters have been pushed down, so
 * a filter that cannot reach the relation selects nothing.
 */
public final class SelectSlicesFromFilter extends OptimizerRules.OptimizerRule<Filter> {

    @Override
    protected LogicalPlan rule(Filter filter) {
        if (filter.child() instanceof EsRelation relation && relation.indexMode() != IndexMode.LOOKUP) {
            SliceSelection slices = selectedSlices(filter.condition());
            if (slices.isRestricted() && slices.equals(relation.slices()) == false) {
                return filter.replaceChild(relation.withSlices(slices));
            }
        }
        return filter;
    }

    /**
     * The slices a filter condition restricts {@code _slice} to, or {@link SliceSelection#UNSPECIFIED} when it names none.
     */
    public static SliceSelection selectedSlices(Expression condition) {
        Set<String> selected = null;
        for (Expression conjunct : Predicates.splitAnd(condition)) {
            Set<String> slices = slicesOf(conjunct);
            if (slices == null) {
                continue;
            }
            if (selected == null) {
                selected = slices;
            } else {
                Set<String> intersection = new LinkedHashSet<>(selected);
                intersection.retainAll(slices);
                // Conditions that contradict each other match no row. Any of them is then a valid selection.
                if (intersection.isEmpty() == false) {
                    selected = intersection;
                }
            }
        }
        return selected == null ? SliceSelection.UNSPECIFIED : SliceSelection.of(List.copyOf(selected));
    }

    /**
     * Whether an expression is a condition that names slices, of one of the recognised forms.
     */
    public static boolean selectsSlices(Expression expression) {
        return slicesOf(expression) != null;
    }

    /**
     * The slices named by {@code _slice == <literal>}, {@code _slice IN (<literals>)} or a disjunction of those, or
     * {@code null} for any other expression.
     */
    private static Set<String> slicesOf(Expression expression) {
        List<Expression> values;
        if (expression instanceof Or or) {
            // A disjunction names slices only when both sides do.
            Set<String> left = slicesOf(or.left());
            Set<String> right = slicesOf(or.right());
            if (left == null || right == null) {
                return null;
            }
            Set<String> slices = new LinkedHashSet<>(left);
            slices.addAll(right);
            return slices;
        } else if (expression instanceof Equals equals) {
            if (isSlice(equals.left())) {
                values = List.of(equals.right());
            } else if (isSlice(equals.right())) {
                values = List.of(equals.left());
            } else {
                return null;
            }
        } else if (expression instanceof In in && isSlice(in.value())) {
            values = in.list();
        } else {
            return null;
        }
        Set<String> slices = new LinkedHashSet<>(values.size());
        for (Expression value : values) {
            String slice = sliceName(value);
            if (slice == null) {
                return null;
            }
            slices.add(slice);
        }
        return slices.isEmpty() ? null : slices;
    }

    private static boolean isSlice(Expression expression) {
        return expression instanceof MetadataAttribute attribute && SliceIndexing.FIELD_NAME.equals(attribute.name());
    }

    /**
     * The slice a literal names, or {@code null} if it is not a valid slice name.
     */
    private static String sliceName(Expression value) {
        if (value instanceof Literal literal
            && DataType.isString(literal.dataType())
            && literal.value() != null
            && literal.value() instanceof List<?> == false) {
            String slice = BytesRefs.toString(literal.value());
            try {
                SliceIndexing.validateUserSliceValue(slice);
            } catch (IllegalArgumentException e) {
                return null;
            }
            return slice;
        }
        return null;
    }
}
