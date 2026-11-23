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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedAny;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Binds {@code NESTED_ANY} to the private nested schema and lowers it to a {@link NestedPredicate}. Name resolution ignores
 * that schema, so {@link #scope} runs while attributes are still unresolved and this rule runs after implicit casting.
 */
public final class BindNestedAny extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
    /**
     * Rewrites nested names inside {@code NESTED_ANY} onto the private schema. Leaves every other name unresolved. A column of the
     * query with the same name replaces the child, as a later definition does everywhere else, so that is an error.
     */
    public static LogicalPlan scope(LogicalPlan plan, List<Attribute> childrenOutput) {
        if (plan instanceof Eval eval) {
            return scopeEval(eval, childrenOutput);
        }
        return plan.transformExpressionsOnlyUp(NestedAny.class, nested -> scope(nested, childrenOutput, Set.of()));
    }

    /** Each assignment sees the columns the earlier assignments of the same {@code EVAL} define. */
    private static LogicalPlan scopeEval(Eval eval, List<Attribute> childrenOutput) {
        if (eval.fields().stream().noneMatch(field -> field.anyMatch(NestedAny.class::isInstance))) {
            return eval;
        }
        Set<String> assigned = new HashSet<>();
        List<Alias> fields = new ArrayList<>();
        for (Alias field : eval.fields()) {
            fields.add((Alias) field.transformUp(NestedAny.class, nested -> scope(nested, childrenOutput, assigned)));
            assigned.add(field.name());
        }
        return fields.equals(eval.fields()) ? eval : new Eval(eval.source(), eval.child(), fields);
    }

    private static Expression scope(NestedAny nested, List<Attribute> childrenOutput, Set<String> assigned) {
        return nested.transformUp(UnresolvedAttribute.class, attribute -> {
            Attribute field = nestedField(attribute, childrenOutput, assigned);
            return field == null ? attribute : field;
        });
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
                LogicalPlan aggregate = NestedRowChecks.aggregateBelow(input);
                if (aggregate != null) {
                    throw error(any, "NESTED_ANY reads each row's nested documents, so it cannot follow [" + aggregate.sourceText() + "]");
                }
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
        Schema schema = Schema.of(input.output());
        Reads reads = readChildren(nested, body, collection, schema);
        if (schema.paths().stream().anyMatch(path -> reads.path().startsWith(path + "."))) {
            throw error(nested, "NESTED_ANY supports one nesting level");
        }
        Inputs inputs = inputs(nested, input, reads, schema);
        Expression bound = body.transformUp(Attribute.class, attribute -> inputs.replacement().getOrDefault(attribute.id(), attribute));
        return new NestedPredicate(nested.source(), reads.path(), bound, inputs.channels(), inputs.relativePaths(), null, configuration);
    }

    /** The private nested schema in the plan's output, by name, and the paths of the nested objects it describes. */
    private record Schema(Map<String, FieldAttribute> byName, Set<String> paths) {
        static Schema of(List<Attribute> output) {
            Map<String, FieldAttribute> byName = new HashMap<>();
            Set<String> paths = new HashSet<>();
            for (Attribute attribute : output) {
                if (attribute instanceof FieldAttribute field && field.field() instanceof NestedScopeField scope) {
                    byName.put(field.name(), field);
                    paths.add(scope.path());
                }
            }
            return new Schema(byName, paths);
        }
    }

    /** The nested path the predicate reads and the attributes of its body that read a child of it. */
    private record Reads(String path, Map<NameId, Attribute> children) {}

    /**
     * Finds the attributes of {@code body} that read children of one nested object. Any other attribute, a parent column or an
     * unmapped name under a nested path, is outside the nested scope and an error.
     */
    private static Reads readChildren(NestedAny nested, Expression body, String collection, Schema schema) {
        Map<NameId, Attribute> attributes = new LinkedHashMap<>();
        body.forEachDown(Attribute.class, attribute -> attributes.putIfAbsent(attribute.id(), attribute));
        Map<NameId, Attribute> children = new LinkedHashMap<>();
        String path = collection;
        for (Attribute attribute : attributes.values()) {
            FieldAttribute field = attribute instanceof FieldAttribute f ? f : null;
            FieldAttribute read = field == null ? null : schemaOf(field, schema.byName());
            if (read != null && read.field() instanceof NestedScopeField scope) {
                if (field.hasTypeConflicts()) {
                    throw error(field, ((UnsupportedAttribute) field.flagTypeConflicts()).unresolvedMessage());
                }
                if (scope.conflict() != null) {
                    throw error(nested, "NESTED_ANY cannot read [" + publicName(read) + "]: " + scope.conflict());
                }
                if (path != null && path.equals(scope.path()) == false) {
                    throw error(nested, "NESTED_ANY requires one nested path");
                }
                path = scope.path();
                children.put(attribute.id(), attribute);
            } else {
                throw error(nested, "Field [" + attribute.name() + "] is outside the nested scope");
            }
        }
        if (path == null) {
            throw error(nested, "NESTED_ANY requires a mapped nested field");
        }
        return new Reads(path, children);
    }

    /** What the predicate loads from each child, and what its body reads in place of the attribute that named it. */
    private record Inputs(List<Attribute> channels, List<String> relativePaths, Map<NameId, Attribute> replacement) {}

    private static Inputs inputs(NestedAny nested, LogicalPlan input, Reads reads, Schema schema) {
        List<Attribute> channels = new ArrayList<>();
        List<String> relativePaths = new ArrayList<>();
        Map<NameId, Attribute> replacement = new HashMap<>();
        for (Attribute attribute : reads.children().values()) {
            FieldAttribute field = (FieldAttribute) attribute;
            FieldAttribute read = schemaOf(field, schema.byName());
            NestedScopeField scope = (NestedScopeField) read.field();
            if (scope.kind() != NestedScopeField.Kind.VALUE) {
                throw error(nested, "NESTED_ANY requires a predicate over nested fields, not [" + publicName(read) + "]");
            }
            if (NestedRowChecks.fromLookupIndex(input, read.id())) {
                throw lookupError(nested, publicName(read));
            }
            relativePaths.add(read.name().substring(NestedScopeField.PREFIX.length() + reads.path().length() + 1));
            if (field.field() instanceof UnionTypeEsField) {
                channels.add(field);
            } else {
                ReferenceAttribute ref = new ReferenceAttribute(attribute.source(), attribute.name(), attribute.dataType());
                channels.add(ref);
                replacement.put(attribute.id(), ref);
            }
        }
        return new Inputs(channels, relativePaths, replacement);
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
                throw error(nested, "NESTED_ANY cannot read [" + scope.path() + "]: " + scope.conflict());
            }
            if (replacedByColumn(input.output(), scope.path())) {
                throw error(nested, "NESTED_ANY cannot use [" + scope.path() + "]: a column of that name replaces it here");
            }
            if (NestedRowChecks.fromLookupIndex(input, field.id())) {
                throw lookupError(nested, scope.path());
            }
            return scope.path();
        }
        throw error(nested, "NESTED_ANY requires a nested collection first, not [" + named.sourceText() + "]");
    }

    /** Whether a column of the query, not a private schema entry, has this name. */
    private static boolean replacedByColumn(List<Attribute> output, String name) {
        return output.stream().anyMatch(column -> column.synthetic() == false && column.name().equals(name));
    }

    private static Attribute nestedField(UnresolvedAttribute attribute, List<Attribute> output, Set<String> assigned) {
        String scopedName = NestedScopeField.PREFIX + attribute.name();
        for (Attribute candidate : output) {
            if (candidate instanceof FieldAttribute field
                && field.name().equals(scopedName)
                && field.field() instanceof NestedScopeField scope) {
                if (assigned.contains(attribute.name()) || replacedByColumn(output, attribute.name())) {
                    throw error(attribute, "NESTED_ANY cannot use [" + attribute.name() + "]: a column of that name replaces it here");
                }
                if (scope.leaf() instanceof UnsupportedEsField unsupported) {
                    return new UnsupportedAttribute(attribute.source(), attribute.name(), unsupported);
                }
                EsField read = scope.leaf() instanceof TypeConflictedField ? scope.leaf() : field.field();
                return new FieldAttribute(
                    attribute.source(),
                    field.name(),
                    null,
                    attribute.name(),
                    read,
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

    private static String publicName(FieldAttribute field) {
        return field.name().substring(NestedScopeField.PREFIX.length());
    }

    private static VerificationException lookupError(NestedAny nested, String name) {
        return error(
            nested,
            "NESTED_ANY cannot read [" + name + "] from a LOOKUP JOIN index; that index's documents are not the rows being read"
        );
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
