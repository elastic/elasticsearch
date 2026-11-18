/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduce;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedStats;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Binds {@code NESTED} to the private nested schema and lowers an ungrouped reduction to a {@link NestedReduce}.
 * {@link #scope} runs while attributes are still unresolved.
 */
public final class BindNested extends Rule<LogicalPlan, LogicalPlan> {
    public static LogicalPlan scope(LogicalPlan plan, List<Attribute> childrenOutput) {
        return plan.transformExpressionsOnlyUp(NestedStats.class, nested -> nested.transformUp(UnresolvedAttribute.class, attribute -> {
            Attribute field = nestedField(attribute, childrenOutput);
            return field == null ? attribute : field;
        }));
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (containsNested(plan)) {
            plan.forEachExpressionDown(NamedExpression.class, expression -> {
                if (expression.synthetic() == false
                    && (expression.name().startsWith(NestedScopeField.PREFIX) || expression.name().equals(NestedScopeField.NAMESPACE))) {
                    throw error(expression, "NESTED uses reserved internal schema names");
                }
            });
        }
        return plan.transformUp(p -> {
            NestedStats nested = find(p);
            if (nested == null) {
                return p;
            }
            if (p instanceof Filter == false && p instanceof Eval == false) {
                throw error(nested, "NESTED is supported in EVAL and WHERE");
            }
            if (p.childrenResolved() == false) {
                return p;
            }
            LogicalPlan input = p.children().getFirst();
            return p.transformExpressionsOnlyUp(NestedStats.class, stats -> bind(stats, input));
        });
    }

    private static boolean containsNested(LogicalPlan plan) {
        boolean[] found = { false };
        plan.forEachDown(node -> {
            if (find(node) != null) {
                found[0] = true;
            }
        });
        return found[0];
    }

    private static NestedStats find(LogicalPlan plan) {
        NestedStats[] found = new NestedStats[1];
        plan.forEachExpression(NestedStats.class, nested -> found[0] = nested);
        return found[0];
    }

    private static Expression bind(NestedStats nested, LogicalPlan input) {
        if (nested.children().size() > 2) {
            throw error(nested, "NESTED takes a reduction, or a collection and a reduction");
        }
        Expression body = nested.body();
        if (body.anyMatch(e -> e instanceof NestedStats || e instanceof FullTextFunction)) {
            throw error(nested, "NESTED does not allow inner scopes or full-text functions");
        }
        checkReadsIndexRows(input, nested);
        if (body instanceof Sum || body instanceof Count || body instanceof Min || body instanceof Max || body instanceof Avg) {
            // The reductions this slice evaluates.
        } else if (body.anyMatch(AggregateFunction.class::isInstance)) {
            throw error(nested, "NESTED supports COUNT, SUM, MIN, MAX and AVG only, not [" + body.sourceText() + "]");
        } else {
            throw error(nested, "NESTED requires a reduction such as SUM or COUNT, not [" + body.sourceText() + "]");
        }
        if (body instanceof AggregateFunction aggregate) {
            if (aggregate.filter().equals(Literal.TRUE) == false || aggregate.window().equals(AggregateFunction.NO_WINDOW) == false) {
                throw error(nested, "NESTED does not support inner aggregate filters or windows");
            }
        }
        String collection = collection(nested, input);
        if (body instanceof Count count && collection == null && count.field().foldable()) {
            throw error(nested, "NESTED(COUNT(*)) needs the collection to count, such as NESTED(items, COUNT(*))");
        }
        List<Attribute> output = input.output();
        Map<NameId, Attribute> fields = new LinkedHashMap<>();
        body.forEachDown(Attribute.class, attribute -> fields.putIfAbsent(attribute.id(), attribute));
        String path = collection;
        for (Attribute attribute : fields.values()) {
            if (attribute instanceof FieldAttribute field && field.field() instanceof NestedScopeField scope) {
                if (scope.conflict() != null) {
                    throw error(nested, "NESTED cannot read [" + publicName(field) + "]: " + scope.conflict());
                }
                if (collection != null && collection.equals(scope.path()) == false) {
                    throw error(nested, "Field [" + publicName(field) + "] is outside the nested scope [" + collection + "]");
                }
                if (path != null && path.equals(scope.path()) == false) {
                    throw error(nested, "NESTED requires one nested path");
                }
                path = scope.path();
            } else if (attribute.foldable() == false) {
                throw error(nested, "Field [" + attribute.name() + "] is outside the nested scope");
            }
        }
        if (path == null) {
            throw error(nested, "NESTED requires a mapped nested field");
        }
        String scopePath = path;
        for (Attribute attribute : output) {
            if (attribute instanceof FieldAttribute field
                && field.field() instanceof NestedScopeField scope
                && scopePath.startsWith(scope.path() + ".")) {
                throw error(nested, "NESTED supports one nesting level");
            }
        }
        List<Attribute> channels = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        Map<NameId, Attribute> replacement = new HashMap<>();
        for (Attribute attribute : fields.values()) {
            if (attribute instanceof FieldAttribute == false) {
                continue;
            }
            FieldAttribute field = (FieldAttribute) attribute;
            if (field.field() instanceof NestedScopeField scope) {
                if (scope.kind() != NestedScopeField.Kind.VALUE) {
                    throw error(nested, "NESTED requires a reduction over nested fields, not [" + publicName(field) + "]");
                }
            } else {
                throw error(nested, "Field [" + attribute.name() + "] is outside the nested scope");
            }
            if (SUPPORTED.contains(attribute.dataType()) == false) {
                throw error(nested, "NESTED does not support child type [" + attribute.dataType() + "] for [" + publicName(field) + "]");
            }
            if (lookupSourced(input, attribute.id())) {
                throw error(
                    nested,
                    "NESTED cannot read ["
                        + publicName(field)
                        + "] from a LOOKUP JOIN index; that index's documents are not the rows being read"
                );
            }
            ReferenceAttribute ref = new ReferenceAttribute(attribute.source(), attribute.name(), attribute.dataType());
            channels.add(ref);
            replacement.put(attribute.id(), ref);
            paths.add(attribute.name().substring(NestedScopeField.PREFIX.length() + path.length() + 1));
        }
        Expression bound = body.transformUp(Attribute.class, attribute -> replacement.getOrDefault(attribute.id(), attribute));
        if (bound instanceof Sum sum) {
            Expression updated = sum.forTransportVersion(TransportVersion.current());
            if (updated != null) {
                bound = updated;
            }
        }
        return new NestedReduce(nested.source(), path, bound, channels, paths);
    }

    private static final java.util.Set<DataType> SUPPORTED = java.util.Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.DATETIME,
        DataType.IP
    );

    private static String collection(NestedStats nested, LogicalPlan input) {
        Expression named = nested.collection();
        if (named == null) {
            return null;
        }
        if (named instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && scope.kind() == NestedScopeField.Kind.COLLECTION) {
            if (scope.conflict() != null) {
                throw error(nested, "NESTED cannot read [" + publicName(field) + "]: " + scope.conflict());
            }
            if (input.output().stream().anyMatch(attribute -> attribute.synthetic() == false && attribute.name().equals(scope.path()))) {
                throw error(nested, "NESTED cannot use [" + scope.path() + "]: a column of that name replaces it here");
            }
            return scope.path();
        }
        throw error(nested, "NESTED requires a nested collection first, not [" + named.sourceText() + "]");
    }

    private static Attribute nestedField(UnresolvedAttribute attribute, List<Attribute> output) {
        String scopedName = NestedScopeField.PREFIX + attribute.name();
        for (Attribute candidate : output) {
            if (candidate instanceof FieldAttribute field && field.name().equals(scopedName) && field.field() instanceof NestedScopeField) {
                return new FieldAttribute(
                    attribute.source(),
                    field.parentName(),
                    null,
                    field.name(),
                    field.field(),
                    field.nullable(),
                    field.id(),
                    true
                );
            }
        }
        String conflict = null;
        boolean collection = false;
        for (Attribute candidate : output) {
            if (candidate instanceof FieldAttribute field
                && field.field() instanceof NestedScopeField scope
                && scope.path().equals(attribute.name())) {
                collection = true;
                if (scope.conflict() != null) {
                    conflict = scope.conflict();
                }
            }
        }
        if (collection == false) {
            return null;
        }
        NestedScopeField scope = new NestedScopeField(
            attribute.name(),
            DataType.SOURCE,
            Map.of(),
            attribute.name(),
            NestedScopeField.Kind.COLLECTION,
            conflict
        );
        return new FieldAttribute(attribute.source(), NestedScopeField.NAMESPACE, null, scopedName, scope, true);
    }

    private static void checkReadsIndexRows(LogicalPlan input, Expression at) {
        LogicalPlan plan = input;
        while (plan instanceof EsRelation == false && plan.children().isEmpty() == false) {
            if (plan instanceof Aggregate) {
                throw error(at, "NESTED reads each row's nested documents, so it cannot follow [" + plan.sourceText() + "]");
            }
            plan = plan instanceof Join join ? join.left() : plan.children().getFirst();
        }
    }

    private static boolean lookupSourced(LogicalPlan plan, NameId id) {
        if (plan instanceof LookupJoin join) {
            if (containsAttribute(join.right(), id)) {
                return true;
            }
            return containsAttribute(join.left(), id) && lookupSourced(join.left(), id);
        }
        if (plan instanceof EsRelation) {
            return false;
        }
        for (LogicalPlan child : plan.children()) {
            if (containsAttribute(child, id)) {
                return lookupSourced(child, id);
            }
        }
        return false;
    }

    private static boolean containsAttribute(LogicalPlan plan, NameId id) {
        if (plan instanceof EsRelation relation) {
            for (Attribute attribute : relation.output()) {
                if (attribute.id().equals(id)) {
                    return true;
                }
            }
        }
        for (LogicalPlan child : plan.children()) {
            if (containsAttribute(child, id)) {
                return true;
            }
        }
        return false;
    }

    private static String publicName(FieldAttribute field) {
        return field.name().substring(NestedScopeField.PREFIX.length());
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
