/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A nested collection is a column of the rows it belongs to. {@code EVAL o = items} gives it another name and a {@code RENAME}
 * moves it, and {@code NESTED_EXPAND} and {@code NESTED_ANY} read the collection through such a column. Those columns are found
 * by attribute id, so that a later column of the same name, a {@code DROP} or a {@code KEEP} decide what a name means, as they do
 * for any column.
 */
public final class NestedReference {
    private NestedReference() {}

    /** The nested path each collection column in the rows {@code plan} produces stands for, by attribute id. */
    public static Map<NameId, String> references(LogicalPlan plan) {
        Map<NameId, String> references = new HashMap<>();
        plan.forEachUp(p -> p.forEachExpression(Alias.class, alias -> {
            String path = path(alias.child());
            if (path != null) {
                references.put(alias.id(), path);
            } else if (alias.child() instanceof Attribute source && references.containsKey(source.id())) {
                references.put(alias.id(), references.get(source.id()));
            }
        }));
        return references;
    }

    /**
     * Gives the collection a name to the {@code EVAL} assignments that are whole names of a nested collection. A name that is
     * not a collection, or that a column of the rows or an earlier assignment already defines, keeps its ordinary meaning.
     * Returns {@code fields} itself when nothing is bound.
     */
    public static List<Alias> bindCollections(List<Alias> fields, List<Attribute> childrenOutput) {
        Set<String> assigned = new HashSet<>();
        List<Alias> bound = null;
        for (int i = 0; i < fields.size(); i++) {
            Alias field = fields.get(i);
            Alias replacement = assigned.contains(sourceName(field)) ? null : bindCollection(field, childrenOutput);
            if (replacement != null) {
                if (bound == null) {
                    bound = new ArrayList<>(fields);
                }
                bound.set(i, replacement);
            }
            assigned.add(field.name());
        }
        return bound == null ? fields : bound;
    }

    /**
     * The alias of {@code field} over the nested collection its child names, or {@code null} when the child is not the whole name
     * of a collection, or a column of the rows already has that name.
     */
    @Nullable
    public static Alias bindCollection(Alias field, List<Attribute> childrenOutput) {
        if (field.child() instanceof UnresolvedAttribute name) {
            FieldAttribute collection = collection(name, childrenOutput);
            if (collection != null && replacedByColumn(childrenOutput, Set.of(), name.name()) == false) {
                checkName(field, collection, childrenOutput);
                return field.replaceChild(collection);
            }
        }
        return null;
    }

    private static String sourceName(Alias field) {
        return field.child() instanceof UnresolvedAttribute name ? name.name() : "";
    }

    /**
     * Replaces a leading collection column of {@code input} in an attribute's name by the collection it stands for, and returns the
     * attribute itself when its name does not start with one. A column of {@code input} called exactly like a prefix of the name
     * keeps its ordinary meaning, so a collection column never hides a column.
     */
    public static UnaryOperator<UnresolvedAttribute> dereferencer(@Nullable LogicalPlan input) {
        Map<NameId, String> references = input == null ? Map.of() : references(input);
        if (references.isEmpty()) {
            return attribute -> attribute;
        }
        return attribute -> {
            String name = attribute.name();
            for (int end = name.length(); end > 0; end = name.lastIndexOf('.', end - 1)) {
                String prefix = name.substring(0, end);
                for (Attribute column : input.output()) {
                    if (column.name().equals(prefix)) {
                        String path = references.get(column.id());
                        return path == null ? attribute : new UnresolvedAttribute(attribute.source(), path + name.substring(end));
                    }
                }
            }
            return attribute;
        };
    }

    /** The collection a column of {@code input} called {@code name} stands for, or {@code null}. */
    @Nullable
    public static String referencedPath(LogicalPlan input, String name) {
        Map<NameId, String> references = references(input);
        for (Attribute column : input.output()) {
            if (column.name().equals(name)) {
                return references.get(column.id());
            }
        }
        return null;
    }

    /** Whether a column of the rows, not a private schema entry or a collection column, has this name. */
    public static boolean replacedByColumn(List<Attribute> output, Set<NameId> references, String name) {
        return output.stream()
            .anyMatch(column -> column.synthetic() == false && column.name().equals(name) && references.contains(column.id()) == false);
    }

    @Nullable
    private static String path(Expression expression) {
        return expression instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && scope.kind() == NestedScopeField.Kind.COLLECTION ? scope.path() : null;
    }

    @Nullable
    private static FieldAttribute collection(UnresolvedAttribute name, List<Attribute> output) {
        String scopedName = NestedScopeField.PREFIX + name.name();
        for (Attribute candidate : output) {
            if (candidate instanceof FieldAttribute field
                && field.name().equals(scopedName)
                && field.field() instanceof NestedScopeField scope
                && scope.kind() == NestedScopeField.Kind.COLLECTION) {
                return new FieldAttribute(
                    name.source(),
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
        return null;
    }

    private static void checkName(Alias field, FieldAttribute collection, List<Attribute> output) {
        String path = ((NestedScopeField) collection.field()).path();
        for (Attribute attribute : output) {
            if (attribute instanceof FieldAttribute other
                && other.field() instanceof NestedScopeField scope
                && scope.kind() == NestedScopeField.Kind.COLLECTION) {
                boolean ownName = field.name().equals(path) && scope.path().equals(path);
                if (ownName == false && (field.name().equals(scope.path()) || field.name().startsWith(scope.path() + "."))) {
                    throw new VerificationException(
                        List.of(
                            Failure.fail(
                                field,
                                "[" + field.name() + "] cannot name the nested collection [" + path + "]: that name is a nested collection"
                            )
                        )
                    );
                }
            }
        }
    }
}
