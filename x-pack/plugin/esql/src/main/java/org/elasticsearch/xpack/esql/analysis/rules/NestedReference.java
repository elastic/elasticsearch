/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedObject;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * The {@code NESTED_OBJECT} references of a plan and the nested collection each stands for. A reference is the column an
 * {@code EVAL} creates for it, or a {@code RENAME} of that column, found by attribute id so that a later column of the same
 * name, a {@code DROP} or a {@code KEEP} decide what a name means, as they do for any column.
 */
public final class NestedReference {
    private NestedReference() {}

    /** The nested path each reference column in the rows {@code plan} produces stands for, by attribute id. */
    static Map<NameId, String> references(LogicalPlan plan) {
        Map<NameId, String> references = new HashMap<>();
        plan.forEachUp(p -> p.forEachExpression(Alias.class, alias -> {
            if (alias.child() instanceof NestedObject object) {
                String path = path(object);
                if (path != null) {
                    references.put(alias.id(), path);
                }
            } else if (p instanceof Project && alias.child() instanceof Attribute source && references.containsKey(source.id())) {
                references.put(alias.id(), references.get(source.id()));
            }
        }));
        return references;
    }

    /**
     * Replaces a leading reference of {@code input} in an attribute's name by the collection it stands for, and returns the
     * attribute itself when its name does not start with a reference. A column of {@code input} called exactly like a prefix of the
     * name keeps its ordinary meaning, so a reference never hides a column.
     */
    static UnaryOperator<UnresolvedAttribute> dereferencer(@Nullable LogicalPlan input) {
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

    /** The collection a reference column of {@code input} called {@code name} stands for, or {@code null}. */
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

    @Nullable
    private static String path(NestedObject object) {
        return object.collection() instanceof FieldAttribute field
            && field.field() instanceof NestedScopeField scope
            && scope.kind() == NestedScopeField.Kind.COLLECTION ? scope.path() : null;
    }
}
