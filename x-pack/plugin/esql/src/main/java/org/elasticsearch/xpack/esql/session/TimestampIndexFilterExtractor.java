/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.TIMESTAMP_FIELD;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.FILTER;

/**
 * Extracts timestamp filters from an unresolved plan for use as index-filter in field-caps.
 */
final class TimestampIndexFilterExtractor {

    private static final long MILLIS_IN_SECOND = 1_000L;
    private static final long MILLIS_IN_MINUTE = 60_000L;
    private static final long MILLIS_IN_HOUR = 3_600_000L;

    private TimestampIndexFilterExtractor() {}

    @Nullable
    static QueryBuilder extract(LogicalPlan plan, Configuration configuration) {
        return combine(collect(plan, new ArrayList<>(), configuration));
    }

    private static Set<QueryBuilder> collect(LogicalPlan node, List<Expression> filters, Configuration configuration) {
        if (node instanceof Fork fork) {
            List<LogicalPlan> subPlans = fork.children();
            if (subPlans.isEmpty()) {
                return Set.of();
            }
            Set<QueryBuilder> common = new LinkedHashSet<>(collect(subPlans.getFirst(), new ArrayList<>(), configuration));
            for (int i = 1; i < subPlans.size(); i++) {
                if (common.isEmpty()) {
                    break;
                }
                Set<QueryBuilder> other = collect(subPlans.get(i), new ArrayList<>(), configuration);
                common.removeIf(q -> other.contains(q) == false);
            }
            common.addAll(toTimestampQueries(filters, configuration));
            return common;
        }
        if (node instanceof UnresolvedRelation) {
            return toTimestampQueries(filters, configuration);
        }
        if (filterPassthrough(node)) {
            Set<String> shadowed = shadowedNames(node);
            if (shadowed.isEmpty() == false && filters.isEmpty() == false) {
                filters.removeIf(c -> referencesAny(c, shadowed));
            }
        } else {
            filters.clear();
        }
        if (node instanceof Filter filter) {
            filters.addAll(Predicates.splitAnd(filter.condition()));
        }
        Set<QueryBuilder> result = new LinkedHashSet<>();
        for (LogicalPlan child : node.children()) {
            result.addAll(collect(child, new ArrayList<>(filters), configuration));
        }
        return result;
    }

    /**
     * Nodes a filter may move past without changing results
     */
    static boolean filterPassthrough(LogicalPlan plan) {
        return plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof Eval
            || plan instanceof RegexExtract
            || plan instanceof Rename
            || plan instanceof Project // includes KEEP
            || plan instanceof Drop
            || plan instanceof Enrich
            || plan instanceof MvExpand;
    }

    static Set<String> shadowedNames(LogicalPlan plan) {
        Set<String> names = new HashSet<>();
        if (plan instanceof GeneratingPlan<?> generating) {
            for (Attribute attr : generating.generatedAttributes()) {
                names.add(attr.name());
            }
        }
        if (plan instanceof Eval eval) {
            for (Alias alias : eval.fields()) {
                names.add(alias.name());
            }
        }
        if (plan instanceof Rename rename) {
            for (Alias alias : rename.renamings()) {
                names.add(alias.name());
            }
        }
        if (plan instanceof MvExpand mvExpand) {
            Attribute expanded = mvExpand.expanded();
            if (expanded != null) {
                names.add(expanded.name());
            }
        }
        return names;
    }

    private static boolean referencesAny(Expression expression, Set<String> names) {
        return expression.anyMatch(e -> e instanceof Attribute attr && names.contains(attr.name()));
    }

    private static Set<QueryBuilder> toTimestampQueries(List<Expression> filters, Configuration configuration) {
        Set<QueryBuilder> ranges = new LinkedHashSet<>();
        for (Expression filter : filters) {
            QueryBuilder range = toTimestampRange(filter, configuration);
            if (range != null) {
                ranges.add(range);
            }
        }
        return ranges;
    }

    @Nullable
    private static QueryBuilder combine(Set<QueryBuilder> queries) {
        if (queries.isEmpty()) {
            return null;
        }
        if (queries.size() == 1) {
            return queries.iterator().next();
        }
        return Queries.combine(FILTER, new ArrayList<>(queries));
    }

    private static QueryBuilder toTimestampRange(Expression expr, Configuration configuration) {
        if (expr instanceof EsqlBinaryComparison comparison) {
            return toTimestampRange(comparison, configuration);
        }
        return null;
    }

    private static QueryBuilder toTimestampRange(EsqlBinaryComparison comparison, Configuration configuration) {
        Expression left = comparison.left();
        Expression right = comparison.right();
        boolean reversed = false;
        UnresolvedAttribute timestampAttr = null;
        Expression boundExpr = null;

        if (left instanceof UnresolvedAttribute attr && TIMESTAMP_FIELD.equals(attr.name())) {
            timestampAttr = attr;
            boundExpr = right;
        } else if (right instanceof UnresolvedAttribute attr && TIMESTAMP_FIELD.equals(attr.name())) {
            timestampAttr = attr;
            boundExpr = left;
            reversed = true;
        }
        if (timestampAttr == null) {
            return null;
        }

        // Expression must only reference @timestamp (+ foldable bound); no other field refs.
        if (comparison.references().stream().anyMatch(a -> TIMESTAMP_FIELD.equals(a.name()) == false)) {
            return null;
        }

        Object bound = foldTimestampBound(boundExpr);
        if (bound == null) {
            return null;
        }

        EsqlBinaryComparison.BinaryComparisonOperation op = comparison.getFunctionType();
        if (reversed) {
            op = reverse(op);
        }

        RangeQueryBuilder range = new RangeQueryBuilder(TIMESTAMP_FIELD);
        Object rangeValue;
        if (bound instanceof String dateMath) {
            ZoneId zoneId = QuerySettings.TIME_ZONE.get(configuration.resolvedSettings());
            range.timeZone(zoneId.getId());
            rangeValue = dateMath;
        } else if (bound instanceof Number n) {
            rangeValue = n.longValue();
        } else if (bound instanceof BytesRef br) {
            rangeValue = BytesRefs.toString(br);
        } else {
            return null;
        }
        return applyBound(range, op, rangeValue);
    }

    private static EsqlBinaryComparison.BinaryComparisonOperation reverse(EsqlBinaryComparison.BinaryComparisonOperation op) {
        return switch (op) {
            case EQ -> EsqlBinaryComparison.BinaryComparisonOperation.EQ;
            case NEQ -> EsqlBinaryComparison.BinaryComparisonOperation.NEQ;
            case GT -> EsqlBinaryComparison.BinaryComparisonOperation.LT;
            case GTE -> EsqlBinaryComparison.BinaryComparisonOperation.LTE;
            case LT -> EsqlBinaryComparison.BinaryComparisonOperation.GT;
            case LTE -> EsqlBinaryComparison.BinaryComparisonOperation.GTE;
        };
    }

    private static QueryBuilder applyBound(RangeQueryBuilder range, EsqlBinaryComparison.BinaryComparisonOperation op, Object value) {
        return switch (op) {
            case GT -> range.gt(value);
            case GTE -> range.gte(value);
            case LT -> range.lt(value);
            case LTE -> range.lte(value);
            case EQ -> range.gte(value).lte(value);
            case NEQ -> null; // cannot express safely as a single range for index pruning
        };
    }

    static Object foldTimestampBound(Expression expr) {
        if (expr instanceof Literal lit) {
            Object value = lit.value();
            if (value instanceof BytesRef || value instanceof String || value instanceof Number) {
                return value;
            }
            return null;
        }
        if (isNow(expr)) {
            return "now";
        }
        if (expr instanceof Sub sub && isNow(sub.left())) {
            String unit = toDateMathUnit(foldTemporalAmount(sub.right()));
            return unit != null ? "now-" + unit : null;
        }
        if (expr instanceof Add add) {
            if (isNow(add.left())) {
                String unit = toDateMathUnit(foldTemporalAmount(add.right()));
                return unit != null ? "now+" + unit : null;
            }
            if (isNow(add.right())) {
                String unit = toDateMathUnit(foldTemporalAmount(add.left()));
                return unit != null ? "now+" + unit : null;
            }
        }
        return null;
    }

    private static boolean isNow(Expression expr) {
        if (expr instanceof Now) {
            return true;
        }
        if (expr instanceof UnresolvedFunction uf) {
            return "now".equalsIgnoreCase(uf.name());
        }
        return false;
    }

    private static TemporalAmount foldTemporalAmount(Expression expr) {
        if (expr instanceof Literal lit) {
            DataType type = lit.dataType();
            if ((type == DataType.TIME_DURATION || type == DataType.DATE_PERIOD) && lit.value() instanceof TemporalAmount amount) {
                return amount;
            }
        }
        return null;
    }

    static String toDateMathUnit(TemporalAmount amount) {
        if (amount == null) {
            return null;
        }
        if (amount instanceof Duration d) {
            long millis = d.toMillis();
            if (millis <= 0) {
                return null;
            }
            if (millis % MILLIS_IN_HOUR == 0) {
                return (millis / MILLIS_IN_HOUR) + "h";
            }
            if (millis % MILLIS_IN_MINUTE == 0) {
                return (millis / MILLIS_IN_MINUTE) + "m";
            }
            if (millis % MILLIS_IN_SECOND == 0) {
                return (millis / MILLIS_IN_SECOND) + "s";
            }
            return null; // sub-second precision is not useful for index pruning
        }
        if (amount instanceof Period p) {
            int years = p.getYears();
            int months = p.getMonths();
            int days = p.getDays();
            if (years > 0 && months == 0 && days == 0) return years + "y";
            if (months > 0 && years == 0 && days == 0) return months + "M";
            if (days > 0 && years == 0 && months == 0) {
                if (days % 7 == 0) return (days / 7) + "w";
                return days + "d";
            }
        }
        return null;
    }
}
