/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
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

        boolean canRewriteGlob() {
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

        boolean isSingleValue() {
            return values.size() == 1;
        }
    }

    public static Map<String, List<PartitionFilterHint>> extract(LogicalPlan unresolvedPlan) {
        Map<String, List<PartitionFilterHint>> result = new LinkedHashMap<>();
        collectHints(unresolvedPlan, List.of(), result);
        return result;
    }

    private static void collectHints(Node<?> node, List<Expression> ancestorConditions, Map<String, List<PartitionFilterHint>> result) {
        List<Expression> conditions = ancestorConditions;

        if (node instanceof Filter filter) {
            conditions = new ArrayList<>(ancestorConditions);
            conditions.add(filter.condition());
        }

        if (node instanceof UnresolvedExternalRelation rel) {
            String path = extractPath(rel);
            if (path != null && conditions.isEmpty() == false) {
                List<PartitionFilterHint> hints = new ArrayList<>();
                for (Expression condition : conditions) {
                    extractFromExpression(condition, hints);
                }
                if (hints.isEmpty() == false) {
                    result.computeIfAbsent(path, k -> new ArrayList<>()).addAll(hints);
                }
            }
            return;
        }

        for (Object child : node.children()) {
            if (child instanceof Node<?> childNode) {
                collectHints(childNode, conditions, result);
            }
        }
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
