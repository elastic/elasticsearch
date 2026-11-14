/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware.FinishedTranslatable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Narrows the parents Lucene returns for a {@code NESTED_EXPAND | WHERE} on the expanded columns. This needs every shard here
 * that maps the field as nested to copy the objects' fields into the parent, with {@code include_in_parent} or
 * {@code include_in_root}: a query on a child field then matches the parent of every object it matches. It also matches
 * parents whose objects only satisfy the predicate together, so the WHERE still runs on each expanded row.
 * <p>
 * Only predicates that a matching row implies of its parent are pushed: comparisons other than {@code !=}, {@code IN},
 * {@code LIKE}, {@code RLIKE} and {@code IS NOT NULL}, joined with AND or OR. A negation or {@code IS NULL} can hold for one
 * object while the parent's copied values fail it, as when another object has the value. A shard that does not map the
 * field gives null child columns, which no pushed predicate accepts, so it loses no row. NESTED_FLATTEN pushes nothing:
 * its WHERE sees all of a parent's values at once.
 */
public final class PushNestedExpandFilterToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(FilterExec filter, LocalPhysicalOptimizerContext context) {
        if (context.configuration().pragmas().nestedLoader() != QueryPragmas.NestedLoader.INDEX
            || filter.child() instanceof NestedExpandExec == false) {
            return filter;
        }
        NestedExpandExec expand = (NestedExpandExec) filter.child();
        if (expand.flatten() || context.searchStats().nestedCopiedToParent(expand.path()) == false) {
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
                    QueryBuilder query = multiValued(TRANSLATOR_HANDLER.asQuery(predicates, predicate)).toQueryBuilder();
                    if (contains(scan.query(), query) == false) {
                        queries.add(query);
                    }
                }
            }
        }
        if (queries.isEmpty()) {
            return filter;
        }
        return filter.replaceChild(expand.replaceChild(PushNestedPredicateToSource.withQuery(expand.child(), queries)));
    }

    /** The expanded columns whose field every shard here can search, as fields of the parent document. */
    private static Map<NameId, FieldAttribute> childFields(NestedExpandExec expand, SearchStats stats) {
        Map<NameId, FieldAttribute> fields = new HashMap<>();
        for (int i = 0; i < expand.expanded().size(); i++) {
            Attribute column = expand.expanded().get(i);
            String name = expand.path() + "." + expand.paths().get(i);
            if (stats.nestedFieldIsSearchable(new FieldAttribute.FieldName(name))) {
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
            || e instanceof RLike
            || e instanceof IsNotNull) {
            return e.children().stream().allMatch(c -> c instanceof Attribute || c instanceof Literal);
        }
        return false;
    }

    /**
     * Drops the single-value checks. The parent holds the values of all of its objects, so one that matches is enough;
     * a parent with two objects would otherwise never match.
     */
    private static Query multiValued(Query query) {
        if (query instanceof SingleValueQuery single) {
            return multiValued(single.next());
        }
        if (query instanceof BoolQuery bool) {
            return new BoolQuery(
                bool.source(),
                bool.isAnd(),
                bool.queries().stream().map(PushNestedExpandFilterToSource::multiValued).toList()
            );
        }
        return query;
    }

    /** Whether an earlier pass already added {@code query}; this rule runs until the plan stops changing. */
    private static boolean contains(QueryBuilder root, QueryBuilder query) {
        if (query.equals(root)) {
            return true;
        }
        return root instanceof BoolQueryBuilder bool && (bool.filter().contains(query) || bool.must().contains(query));
    }
}
