/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks an unresolved logical plan extracting simple filter predicates from {@link Filter} nodes
 * above {@link UnresolvedExternalRelation} nodes. These hints are used for partition pruning
 * during glob expansion and split discovery.
 *
 * <p>Only extracts predicates with an {@link UnresolvedAttribute} on one side and a {@link Literal}
 * on the other. Supported operators: {@code =}, {@code !=}, {@code >}, {@code >=}, {@code <},
 * {@code <=}, {@code IN}.
 */
public final class PartitionFilterHintExtractor {

    private PartitionFilterHintExtractor() {}

    public enum Operator {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        IN;

        public boolean canRewriteGlob() {
            return this == EQUALS || this == IN;
        }
    }

    public record PartitionFilterHint(String columnName, Operator operator, List<Object> values) {
        public PartitionFilterHint {
            if (columnName == null) {
                throw new IllegalArgumentException("columnName cannot be null");
            }
            if (operator == null) {
                throw new IllegalArgumentException("operator cannot be null");
            }
            values = values != null ? List.copyOf(values) : List.of();
        }

        public boolean isSingleValue() {
            return values.size() == 1;
        }
    }

    public static Map<String, List<PartitionFilterHint>> extract(LogicalPlan unresolvedPlan) {
        Map<String, List<List<PartitionFilterHint>>> perOccurrence = new LinkedHashMap<>();
        collectHints(unresolvedPlan, List.of(), perOccurrence);

        // One listing serves every occurrence of a path, so a folder may be skipped only if EVERY occurrence excludes
        // it. `FROM ds | FORK (WHERE year == 2025) (WHERE ...)` reaches the same relation twice; letting one branch's
        // hint narrow the shared listing would starve the other. An unguarded occurrence contributes no hints and so
        // vetoes the rewrite outright.
        //
        // The intersection is by exact hint equality (column, operator, values), so it keeps only hints two branches
        // spell identically. It does not reason about subsumption: `year == 2025` in one branch and `year >= 2020` in
        // another share no common hint even though the first implies the second, so the rewrite is skipped and the full
        // set is listed. That is conservative — correct, only wider — and semantic subsumption is left as a follow-up.
        Map<String, List<PartitionFilterHint>> result = new LinkedHashMap<>();
        perOccurrence.forEach((path, occurrences) -> {
            List<PartitionFilterHint> common = new ArrayList<>(occurrences.get(0));
            for (int i = 1; i < occurrences.size(); i++) {
                common.retainAll(occurrences.get(i));
            }
            if (common.isEmpty() == false) {
                result.put(path, common);
            }
        });
        return result;
    }

    /**
     * Walks top-down (pipeline-last node first), carrying the conjuncts that guard the relations below.
     *
     * <p>Two things can disqualify a conjunct on the way down, and both are decided by {@link PartitionPruningRule}:
     *
     * <ul>
     *   <li>A node that changes the row count — a {@code LIMIT}, {@code STATS}, {@code SAMPLE}, a join. A {@code WHERE}
     *       above one of those does not commute with it, so nothing collected above may be used to narrow the listing
     *       below it. The accumulator resets.</li>
     *   <li>A node that redefines a name a conjunct depends on — {@code EVAL year = ...} or {@code RENAME x AS year}.
     *       That {@code year} is a row value, not the {@code year=2024/} folder the row came from, so a hint on it must
     *       be dropped before it can rewrite the glob.</li>
     * </ul>
     *
     * <p>Shadowing is judged at the moment the walk passes the redefining node, not accumulated and applied at the
     * relation. The direction matters and is easy to get backwards: a conjunct is only endangered by a node
     * <em>between</em> it and the relation, which — walking top-down — is a node visited <em>after</em> the conjunct was
     * collected. A generating node above the filter is irrelevant. Dropping at the pass-through moment gets both right,
     * and in particular keeps the hint in {@code WHERE year == 2025 | EVAL year = 9}, where the filter reads the
     * partition column and the {@code EVAL} only affects what comes after it.
     */
    private static void collectHints(
        LogicalPlan node,
        List<Expression> guardingConjuncts,
        Map<String, List<List<PartitionFilterHint>>> result
    ) {
        if (node instanceof UnresolvedExternalRelation rel) {
            String path = extractPath(rel);
            if (path != null) {
                List<PartitionFilterHint> hints = new ArrayList<>();
                for (Expression conjunct : guardingConjuncts) {
                    extractFromExpression(conjunct, hints);
                }
                // Registered even when empty: an occurrence with no usable hint must veto the rewrite, not be ignored.
                result.computeIfAbsent(path, k -> new ArrayList<>()).add(hints);
            }
            return;
        }

        List<Expression> conjuncts = PartitionPruningRule.hintTransparent(node) ? guardingConjuncts : List.of();

        Set<String> shadowed = PartitionPruningRule.shadowedNames(node);
        if (shadowed.isEmpty() == false && conjuncts.isEmpty() == false) {
            conjuncts = conjuncts.stream().filter(c -> referencesAny(c, shadowed) == false).toList();
        }

        if (node instanceof Filter filter) {
            List<Expression> extended = new ArrayList<>(conjuncts);
            extended.addAll(Predicates.splitAnd(filter.condition()));
            conjuncts = List.copyOf(extended);
        }

        for (LogicalPlan child : node.children()) {
            collectHints(child, conjuncts, result);
        }
    }

    /** Whether {@code expression} reads any of {@code names} — matched on the attribute name, all a hint has pre-resolution. */
    private static boolean referencesAny(Expression expression, Set<String> names) {
        return expression.anyMatch(e -> e instanceof Attribute attr && names.contains(attr.name()));
    }

    private static void extractFromExpression(Expression expr, List<PartitionFilterHint> hints) {
        for (Expression conjunct : Predicates.splitAnd(expr)) {
            if (conjunct instanceof EsqlBinaryComparison comparison) {
                extractFromComparison(comparison, hints);
            } else if (conjunct instanceof In in) {
                extractFromIn(in, hints);
            }
        }
    }

    private static void extractFromComparison(EsqlBinaryComparison comparison, List<PartitionFilterHint> hints) {
        Expression left = comparison.left();
        Expression right = comparison.right();

        String columnName = null;
        Object literalValue = null;
        boolean reversed = false;

        if (left instanceof UnresolvedAttribute attr && right instanceof Literal lit) {
            columnName = attr.name();
            literalValue = lit.value();
        } else if (left instanceof Literal lit && right instanceof UnresolvedAttribute attr) {
            columnName = attr.name();
            literalValue = lit.value();
            reversed = true;
        }

        if (columnName == null) {
            return;
        }

        Operator operator = toOperator(comparison, reversed);
        if (operator != null) {
            hints.add(new PartitionFilterHint(columnName, operator, List.of(normalizeValue(literalValue))));
        }
    }

    private static void extractFromIn(In in, List<PartitionFilterHint> hints) {
        Expression value = in.value();
        if (value instanceof UnresolvedAttribute == false) {
            return;
        }
        UnresolvedAttribute attr = (UnresolvedAttribute) value;

        List<Object> literalValues = new ArrayList<>();
        for (Expression listItem : in.list()) {
            if (listItem instanceof Literal lit) {
                literalValues.add(normalizeValue(lit.value()));
            } else {
                return;
            }
        }

        if (literalValues.isEmpty() == false) {
            hints.add(new PartitionFilterHint(attr.name(), Operator.IN, literalValues));
        }
    }

    private static Object normalizeValue(Object value) {
        return value instanceof BytesRef br ? BytesRefs.toString(br) : value;
    }

    private static Operator toOperator(EsqlBinaryComparison comparison, boolean reversed) {
        return switch (comparison) {
            case Equals ignored -> Operator.EQUALS;
            case NotEquals ignored -> Operator.NOT_EQUALS;
            case GreaterThan ignored -> reversed ? Operator.LESS_THAN : Operator.GREATER_THAN;
            case GreaterThanOrEqual ignored -> reversed ? Operator.LESS_THAN_OR_EQUAL : Operator.GREATER_THAN_OR_EQUAL;
            case LessThan ignored -> reversed ? Operator.GREATER_THAN : Operator.LESS_THAN;
            case LessThanOrEqual ignored -> reversed ? Operator.GREATER_THAN_OR_EQUAL : Operator.LESS_THAN_OR_EQUAL;
            default -> null;
        };
    }

    private static String extractPath(UnresolvedExternalRelation rel) {
        Expression tablePath = rel.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }
}
