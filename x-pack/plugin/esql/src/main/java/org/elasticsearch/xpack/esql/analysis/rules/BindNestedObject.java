/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedObject;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks that every {@code NESTED_OBJECT} names a mapped nested collection the rows can read, and that it is the whole value of an
 * {@code EVAL} column. {@link #scope} runs while attributes are still unresolved, because name resolution cannot see nested
 * fields. The reference stays in the plan for the commands that read through it, until {@link RemoveNestedReferences}.
 */
public final class BindNestedObject extends Rule<LogicalPlan, LogicalPlan> {
    /** Points the name inside {@code NESTED_OBJECT} at the private nested schema. Leaves every other name unresolved. */
    public static LogicalPlan scope(LogicalPlan plan, List<Attribute> childrenOutput) {
        return plan.transformExpressionsOnlyUp(NestedObject.class, object -> object.transformUp(UnresolvedAttribute.class, attribute -> {
            Attribute field = nestedField(attribute, childrenOutput);
            return field == null ? attribute : field;
        }));
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan.anyMatch(p -> objects(p).isEmpty() == false) == false) {
            return plan;
        }
        plan.forEachDown(p -> {
            List<NestedObject> objects = objects(p);
            if (objects.isEmpty()) {
                return;
            }
            if (p instanceof Eval == false) {
                throw error(objects.getFirst(), "NESTED_OBJECT is supported in EVAL");
            }
            Eval eval = (Eval) p;
            for (Alias field : eval.fields()) {
                if (field.child() instanceof NestedObject object) {
                    check(object, field, eval);
                } else {
                    field.child()
                        .forEachDown(
                            NestedObject.class,
                            object -> { throw error(object, "NESTED_OBJECT must be the whole value of an EVAL column"); }
                        );
                }
            }
        });
        return plan;
    }

    private static List<NestedObject> objects(LogicalPlan plan) {
        List<NestedObject> objects = new ArrayList<>();
        plan.forEachExpression(NestedObject.class, objects::add);
        return objects;
    }

    private static void check(NestedObject object, Alias field, Eval eval) {
        if (object.collection().resolved() == false) {
            return;
        }
        LogicalPlan input = eval.child();
        checkReadsIndexRows(input, object);
        if (object.collection() instanceof FieldAttribute named
            && named.field() instanceof NestedScopeField namedScope
            && namedScope.conflict() != null) {
            throw error(object, "NESTED_OBJECT cannot read [" + publicName(named) + "]: " + namedScope.conflict());
        }
        if ((object.collection() instanceof FieldAttribute attribute
            && attribute.field() instanceof NestedScopeField scope
            && scope.kind() == NestedScopeField.Kind.COLLECTION) == false) {
            throw error(object, "NESTED_OBJECT requires a nested collection, not [" + object.collection().sourceText() + "]");
        }
        FieldAttribute collection = (FieldAttribute) object.collection();
        String path = ((NestedScopeField) collection.field()).path();
        if (lookupSourced(input, collection.id())) {
            throw error(
                object,
                "NESTED_OBJECT cannot read [" + path + "] from a LOOKUP JOIN index; that index's documents are not the rows being read"
            );
        }
        if (input.output().stream().anyMatch(a -> a.synthetic() == false && a.name().equals(path))
            && NestedReference.referencedPath(input, path) == null) {
            throw error(object, "NESTED_OBJECT cannot use [" + path + "]: a column of that name replaces it here");
        }
        for (Attribute attribute : input.output()) {
            if (attribute instanceof FieldAttribute other
                && other.field() instanceof NestedScopeField otherScope
                && otherScope.kind() == NestedScopeField.Kind.COLLECTION) {
                boolean ownName = field.name().equals(path) && otherScope.path().equals(path);
                if (ownName == false && (field.name().equals(otherScope.path()) || field.name().startsWith(otherScope.path() + "."))) {
                    throw error(object, "NESTED_OBJECT cannot be named [" + field.name() + "]: that name is a nested collection");
                }
                if (path.startsWith(otherScope.path() + ".")) {
                    throw error(object, "NESTED_OBJECT supports one nesting level");
                }
            }
        }
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
        return null;
    }

    /** Child documents belong to a parent row. A STATS replaces those rows with groups, which have none. */
    private static void checkReadsIndexRows(LogicalPlan input, Expression at) {
        LogicalPlan plan = input;
        while (plan instanceof EsRelation == false && plan.children().isEmpty() == false) {
            if (plan instanceof Aggregate) {
                throw error(at, "NESTED_OBJECT reads each row's nested documents, so it cannot follow [" + plan.sourceText() + "]");
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

    private static String publicName(FieldAttribute field) {
        return field.name().substring(NestedScopeField.PREFIX.length());
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
