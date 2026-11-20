/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedAny;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Binds {@code NESTED_ANY} to the private nested schema and lowers it to a {@link NestedPredicate}. Name resolution ignores
 * that schema, so {@link #scope} runs while attributes are still unresolved and this rule runs after implicit casting.
 */
public final class BindNestedAny extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
    /** Rewrites nested names inside {@code NESTED_ANY} onto the private schema. Leaves every other name unresolved. */
    public static LogicalPlan scope(LogicalPlan plan, List<Attribute> childrenOutput) {
        return plan.transformExpressionsOnlyUp(NestedAny.class, nested -> nested.transformUp(UnresolvedAttribute.class, attribute -> {
            Attribute field = nestedField(attribute, childrenOutput);
            return field == null ? attribute : field;
        }));
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return plan.transformUp(p -> {
            NestedAny nested = find(p);
            if (nested == null) {
                return p;
            }
            if (context.minimumVersion().supports(NestedPredicate.VERSION) == false) {
                throw error(nested, "NESTED_ANY requires all nodes to support nested predicates");
            }
            if (p instanceof Filter == false && p instanceof Eval == false) {
                throw error(nested, "NESTED_ANY is supported in EVAL and WHERE");
            }
            if (p.childrenResolved() == false) {
                return p;
            }
            LogicalPlan input = p.children().getFirst();
            return p.transformExpressionsOnlyUp(NestedAny.class, any -> {
                // An aggregate drops the nested documents before the fields can be bound, so report that first.
                checkReadsIndexRows(input, any);
                return any.resolved() ? bind(any, input, context.configuration()) : any;
            });
        });
    }

    private static NestedAny find(LogicalPlan plan) {
        NestedAny[] found = new NestedAny[1];
        plan.forEachExpression(NestedAny.class, nested -> found[0] = nested);
        return found[0];
    }

    private static Expression bind(NestedAny nested, LogicalPlan input, Configuration configuration) {
        Expression body = nested.body();
        String collection = collection(nested, input);
        List<Attribute> output = input.output();
        Map<String, FieldAttribute> schema = new HashMap<>();
        for (Attribute attribute : output) {
            if (attribute instanceof FieldAttribute field && field.field() instanceof NestedScopeField) {
                schema.put(field.name(), field);
            }
        }
        Map<NameId, Attribute> fields = new LinkedHashMap<>();
        body.forEachDown(Attribute.class, attribute -> fields.putIfAbsent(attribute.id(), attribute));
        String path = collection;
        for (Attribute attribute : fields.values()) {
            FieldAttribute field = attribute instanceof FieldAttribute f ? f : null;
            FieldAttribute read = field == null ? null : schemaOf(field, schema);
            if (read != null && read.field() instanceof NestedScopeField scope) {
                if (field.hasTypeConflicts()) {
                    throw error(field, ((UnsupportedAttribute) field.flagTypeConflicts()).unresolvedMessage());
                }
                if (scope.conflict() != null) {
                    throw error(nested, "NESTED_ANY cannot read [" + publicName(read) + "]: " + scope.conflict());
                }
                if (scope.kind() != NestedScopeField.Kind.VALUE || scope.path().equals(path == null ? scope.path() : path) == false) {
                    if (path != null && path.equals(scope.path()) == false) {
                        throw error(nested, "NESTED_ANY requires one nested path");
                    }
                }
                if (collection != null && collection.equals(scope.path()) == false) {
                    throw error(nested, "Field [" + publicName(read) + "] is outside the nested scope [" + collection + "]");
                }
                if (path != null && path.equals(scope.path()) == false) {
                    throw error(nested, "NESTED_ANY requires one nested path");
                }
                path = scope.path();
            } else {
                throw error(nested, "Field [" + attribute.name() + "] is outside the nested scope");
            }
        }
        if (path == null) {
            throw error(nested, "NESTED_ANY requires a mapped nested field");
        }
        String scopePath = path;
        for (Attribute attribute : output) {
            if (attribute instanceof FieldAttribute field
                && field.field() instanceof NestedScopeField scope
                && scopePath.startsWith(scope.path() + ".")) {
                throw error(nested, "NESTED_ANY supports one nesting level");
            }
        }
        List<Attribute> channels = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        Map<NameId, Attribute> replacement = new HashMap<>();
        for (Attribute attribute : fields.values()) {
            FieldAttribute field = (FieldAttribute) attribute;
            FieldAttribute read = schemaOf(field, schema);
            NestedScopeField scope = (NestedScopeField) read.field();
            if (scope.kind() != NestedScopeField.Kind.VALUE) {
                throw error(nested, "NESTED_ANY requires a predicate over nested fields, not [" + publicName(read) + "]");
            }
            if (lookupSourced(input, read.id())) {
                throw error(
                    nested,
                    "NESTED_ANY cannot read ["
                        + publicName(read)
                        + "] from a LOOKUP JOIN index; that index's documents are not the rows being read"
                );
            }
            paths.add(read.name().substring(NestedScopeField.PREFIX.length() + path.length() + 1));
            if (field.field() instanceof UnionTypeEsField) {
                channels.add(field);
            } else {
                ReferenceAttribute ref = new ReferenceAttribute(attribute.source(), attribute.name(), attribute.dataType());
                channels.add(ref);
                replacement.put(attribute.id(), ref);
            }
        }
        Expression bound = body.transformUp(Attribute.class, attribute -> replacement.getOrDefault(attribute.id(), attribute));
        return new NestedPredicate(nested.source(), path, bound, channels, paths, configuration);
    }

    /** The named collection, or {@code null} when the predicate's fields name the path. */
    private static String collection(NestedAny nested, LogicalPlan input) {
        Expression named = nested.collection();
        if (named == null) {
            return null;
        }
        if (named instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && scope.kind() == NestedScopeField.Kind.COLLECTION) {
            if (scope.conflict() != null) {
                throw error(nested, "NESTED_ANY cannot read [" + publicName(field) + "]: " + scope.conflict());
            }
            if (input.output().stream().anyMatch(attribute -> attribute.synthetic() == false && attribute.name().equals(scope.path()))) {
                throw error(nested, "NESTED_ANY cannot use [" + scope.path() + "]: a column of that name replaces it here");
            }
            return scope.path();
        }
        throw error(nested, "NESTED_ANY requires a nested collection first, not [" + named.sourceText() + "]");
    }

    private static Attribute nestedField(UnresolvedAttribute attribute, List<Attribute> output) {
        String scopedName = NestedScopeField.PREFIX + attribute.name();
        for (Attribute candidate : output) {
            if (candidate instanceof FieldAttribute field
                && field.name().equals(scopedName)
                && field.field() instanceof NestedScopeField scope) {
                if (scope.leaf() instanceof UnsupportedEsField unsupported) {
                    return new UnsupportedAttribute(attribute.source(), attribute.name(), unsupported);
                }
                if (scope.leaf() instanceof TypeConflictedField) {
                    return new FieldAttribute(
                        attribute.source(),
                        field.name(),
                        null,
                        attribute.name(),
                        scope.leaf(),
                        field.nullable(),
                        field.id(),
                        true
                    );
                }
                return new FieldAttribute(
                    attribute.source(),
                    field.name(),
                    null,
                    attribute.name(),
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
            conflict,
            null
        );
        return new FieldAttribute(attribute.source(), NestedScopeField.NAMESPACE, null, scopedName, scope, true);
    }

    private static void checkReadsIndexRows(LogicalPlan input, Expression at) {
        LogicalPlan plan = input;
        while (plan instanceof EsRelation == false && plan.children().isEmpty() == false) {
            if (plan instanceof Aggregate) {
                throw error(at, "NESTED_ANY reads each row's nested documents, so it cannot follow [" + plan.sourceText() + "]");
            }
            plan = plan instanceof Join join ? join.left() : plan.children().getFirst();
        }
    }

    /** The collection attribute comes from the right side of a LOOKUP JOIN, whose documents are not the input rows. */
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

    /**
     * The private schema attribute a body attribute reads: itself, or for a child whose type conflicts across indices, or a cast of
     * one, the schema attribute named by its parent. {@code null} for a name outside the nested scope.
     */
    private static FieldAttribute schemaOf(FieldAttribute field, Map<String, FieldAttribute> schema) {
        FieldAttribute named = field.parentName() == null ? null : schema.get(field.parentName());
        if (named != null) {
            return named;
        }
        return field.field() instanceof NestedScopeField ? field : null;
    }

    /** Whether the attribute reads a child of a nested collection through the private schema. */
    public static boolean readsNestedChild(FieldAttribute field) {
        return field.parentName() != null && field.parentName().startsWith(NestedScopeField.PREFIX);
    }

    private static String publicName(FieldAttribute field) {
        return field.name().substring(NestedScopeField.PREFIX.length());
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
