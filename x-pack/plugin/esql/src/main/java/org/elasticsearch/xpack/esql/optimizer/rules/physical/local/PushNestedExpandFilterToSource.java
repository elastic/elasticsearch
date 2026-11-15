/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware.FinishedTranslatable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Narrows the parents Lucene returns for a {@code NESTED_EXPAND | WHERE} with a nested query over the actual child
 * documents. No copy of the child values in the parent is needed. The WHERE still runs on every expanded row: a selected
 * parent can also have children that do not match. Single-value checks remain inside the nested query, where they apply
 * to each child, rather than to all the values of the parent.
 * <p>
 * Only the supported positive, null-rejecting predicates are pushed. A parent without children keeps a null row during
 * expansion; a predicate accepting that row could not be replaced by child existence. Null-sensitive predicates also
 * stay in compute until existence-query rewriting preserves nested scope. NESTED_FLATTEN pushes nothing: its WHERE
 * sees all of a parent's values at once.
 */
public final class PushNestedExpandFilterToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(FilterExec filter, LocalPhysicalOptimizerContext context) {
        if (filter.child() instanceof NestedExpandExec == false) {
            return filter;
        }
        NestedExpandExec expand = (NestedExpandExec) filter.child();
        if (expand.flatten()) {
            return filter;
        }
        EsQueryExec scan = PushNestedPredicateToSource.scan(expand.child());
        if (scan == null) {
            return filter;
        }
        Map<NameId, FieldAttribute> fields = childFields(expand, context.searchStats());
        var predicates = PushNestedPredicateToSource.childFields(context.flags());
        List<QueryBuilder> queries = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            if (conjunct.references().isEmpty() == false
                && conjunct.references().stream().allMatch(a -> fields.containsKey(a.id()))
                && positive(conjunct)) {
                Expression predicate = conjunct.transformUp(Attribute.class, a -> fields.get(a.id()));
                if (translatable(predicate, predicates).finish() != FinishedTranslatable.NO) {
                    queries.add(TRANSLATOR_HANDLER.asQuery(predicates, predicate).toQueryBuilder());
                }
            }
        }
        if (queries.isEmpty()) {
            return filter;
        }
        // All these conjuncts filter the same expanded row, so require one child to match them together.
        BoolQueryBuilder childQuery = new BoolQueryBuilder();
        queries.forEach(childQuery::filter);
        QueryBuilder query = new NestedQueryBuilder(expand.path(), queries.size() == 1 ? queries.getFirst() : childQuery, ScoreMode.None)
            .ignoreUnmapped(true);
        if (contains(scan.query(), query)) {
            return filter;
        }
        return filter.replaceChild(expand.replaceChild(PushNestedPredicateToSource.withQuery(expand.child(), List.of(query))));
    }

    /** Expanded columns whose mapped child values can safely be compared by Lucene on every applicable shard. */
    private static Map<NameId, FieldAttribute> childFields(NestedExpandExec expand, SearchStats stats) {
        Map<NameId, FieldAttribute> fields = new HashMap<>();
        for (int i = 0; i < expand.expanded().size(); i++) {
            Attribute column = expand.expanded().get(i);
            String name = expand.path() + "." + expand.paths().get(i);
            if (stats.nestedFieldIsQueryable(new FieldAttribute.FieldName(name), column.dataType())) {
                EsField field = new EsField(name, column.dataType(), Map.of(), true, EsField.TimeSeriesFieldType.NONE);
                fields.put(column.id(), new FieldAttribute(column.source(), name, field));
            }
        }
        return fields;
    }

    private static boolean positive(Expression e) {
        if (e instanceof And || e instanceof Or) {
            return e.children().stream().allMatch(PushNestedExpandFilterToSource::positive);
        }
        if (e instanceof Equals
            || e instanceof GreaterThan
            || e instanceof GreaterThanOrEqual
            || e instanceof LessThan
            || e instanceof LessThanOrEqual
            || e instanceof In
            || e instanceof WildcardLike
            || e instanceof RLike) {
            return e.children().stream().allMatch(c -> c instanceof Attribute || c instanceof Literal);
        }
        return false;
    }

    /** Whether an earlier pass already added {@code query}; this rule runs until the plan stops changing. */
    private static boolean contains(QueryBuilder root, QueryBuilder query) {
        if (query.equals(root)) {
            return true;
        }
        return root instanceof BoolQueryBuilder bool && (bool.filter().contains(query) || bool.must().contains(query));
    }
}
