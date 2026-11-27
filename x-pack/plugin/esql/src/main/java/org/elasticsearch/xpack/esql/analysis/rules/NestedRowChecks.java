/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedContainerField;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * Questions the commands that read a nested object's child documents, {@code NESTED_EXPAND} and {@code NESTED_ANY}, ask about
 * the plan below them. Child documents belong to a row of the index the query reads, so each question is about whether the rows
 * of the plan still are those.
 */
public final class NestedRowChecks {
    private NestedRowChecks() {}

    /**
     * Whether {@code id} comes from the right side of a LOOKUP JOIN, whose documents are not the rows of the plan below the
     * command.
     */
    public static boolean fromLookupIndex(LogicalPlan plan, NameId id) {
        if (plan instanceof LookupJoin join) {
            if (containsAttribute(join.right(), id)) {
                return true;
            }
            return containsAttribute(join.left(), id) && fromLookupIndex(join.left(), id);
        }
        if (plan instanceof EsRelation) {
            return false;
        }
        for (LogicalPlan child : plan.children()) {
            if (containsAttribute(child, id)) {
                return fromLookupIndex(child, id);
            }
        }
        return false;
    }

    /**
     * The aggregate that turned the index's rows into groups, which have no child documents, or {@code null} when the rows
     * of the plan are still those of the index.
     */
    public static LogicalPlan aggregateBelow(LogicalPlan input) {
        LogicalPlan plan = input;
        while (plan instanceof EsRelation == false && plan.children().isEmpty() == false) {
            if (plan instanceof Aggregate) {
                return plan;
            }
            plan = plan instanceof Join join ? join.left() : plan.children().getFirst();
        }
        return null;
    }

    /**
     * A column for the child documents of the lookup document that owns {@code path}. Its name is unique, because a command that
     * reads the same collection again brings its own column and the plan merges columns by name.
     */
    public static FieldAttribute newContainer(Source source, String path, List<String> paths, List<DataType> types) {
        NameId id = new NameId();
        String name = NestedContainerField.NAME_PREFIX + path + "#" + id;
        return new FieldAttribute(source, null, null, name, new NestedContainerField(name, path, paths, types), Nullability.TRUE, id, true);
    }

    /** Makes the right side of the LOOKUP JOIN that owns {@code collection} output {@code container}. */
    public static LogicalPlan withContainer(LogicalPlan plan, NameId collection, FieldAttribute container) {
        return plan.transformUp(LookupJoin.class, join -> {
            if (containsAttribute(join.right(), collection) == false) {
                return join;
            }
            LogicalPlan right = join.right().transformUp(EsRelation.class, relation -> {
                if (containsAttribute(relation, collection) == false) {
                    return relation;
                }
                List<Attribute> attributes = new ArrayList<>(relation.output());
                attributes.add(container);
                return relation.withAttributes(attributes);
            });
            return join.replaceChildren(join.left(), right);
        });
    }

    /** Whether a relation in {@code plan} outputs the attribute {@code id}. */
    public static boolean containsAttribute(LogicalPlan plan, NameId id) {
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
}
