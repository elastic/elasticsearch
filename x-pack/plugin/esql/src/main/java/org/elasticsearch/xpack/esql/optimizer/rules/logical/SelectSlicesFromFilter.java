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
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Records on a source the slices that the filter sitting on it restricts it to. The filter stays in the plan: the slices
 * only tell the engine which shards to query and which slices each search context reads.
 * <p>
 * Slices are read from the conditions AND'd at the top level of the filter that have the form {@code _slice == <literal>}
 * or {@code _slice IN (<literals>)}, or are a disjunction of those. The rule runs once the filters have been pushed down, so
 * a filter that cannot reach the relation selects nothing.
 * <p>
 * A knn function searches the slices of its source, so the conditions that select them are removed from its filters. The
 * data nodes build the filters of a knn function again when they optimize the plan, so the rule also runs there: it does not
 * read the slices again, and only removes those conditions when they select the slices the source carries.
 */
public final class SelectSlicesFromFilter extends OptimizerRules.OptimizerRule<Filter> implements OptimizerRules.LocalAware<Filter> {

    /** Whether the slices are read from the filter, or only those already recorded on the source are trusted. */
    private final boolean select;

    public SelectSlicesFromFilter() {
        this(true);
    }

    private SelectSlicesFromFilter(boolean select) {
        this.select = select;
    }

    @Override
    public Rule<Filter, LogicalPlan> local() {
        return new SelectSlicesFromFilter(false);
    }

    @Override
    protected LogicalPlan rule(Filter filter) {
        if (filter.child() instanceof EsRelation relation && relation.indexMode() != IndexMode.LOOKUP) {
            SliceSelection slices = selectedSlices(filter.condition());
            if (slices.isRestricted() == false || (select == false && slices.equals(relation.slices()) == false)) {
                return filter;
            }
            EsRelation source = slices.equals(relation.slices()) ? relation : relation.withSlices(slices);
            Expression condition = removeFromKnnFilters(filter.condition());
            if (source != relation || condition != filter.condition()) {
                return new Filter(filter.source(), source, condition);
            }
        }
        return filter;
    }

    /**
     * Removes the conditions that select the slices of the source from the filters of the knn functions of the condition.
     */
    private static Expression removeFromKnnFilters(Expression condition) {
        List<Expression> selecting = Predicates.splitAnd(condition).stream().filter(c -> slicesOf(c) != null).toList();
        return condition.transformDown(Knn.class, knn -> {
            List<Expression> filters = new ArrayList<>(knn.filterExpressions().size());
            for (Expression filter : knn.filterExpressions()) {
                List<Expression> conjuncts = Predicates.splitAnd(filter);
                List<Expression> remaining = conjuncts.stream().filter(c -> selecting.contains(c) == false).toList();
                if (remaining.size() == conjuncts.size()) {
                    filters.add(filter);
                } else if (remaining.isEmpty() == false) {
                    filters.add(Predicates.combineAnd(remaining));
                }
            }
            return filters.equals(knn.filterExpressions()) ? knn : knn.withFilters(filters);
        });
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
            // a valid slice name holds no whitespace, so it is the name SliceSelection keeps once it has trimmed it
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
