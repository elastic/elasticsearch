/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.expression.unit.Unit;
import org.elasticsearch.xpack.esql.expression.unit.UnitAware;
import org.elasticsearch.xpack.esql.index.FieldSemantics;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;

import java.util.HashMap;
import java.util.Map;

/**
 * Derives the unit of every column of a plan from the {@code meta.unit} of the mapped fields it reads.
 * <p>
 * Each expression owns its rule through {@link UnitAware}; an expression without one has an unknown unit,
 * and so does everything computed from it.
 */
public final class ColumnUnits {

    private ColumnUnits() {}

    /**
     * Returns the unit of every attribute of {@code plan} whose unit is known, keyed by attribute id.
     *
     * @param fields the semantics of the mapped fields, see {@link ColumnSemantics#collect}
     */
    public static Map<NameId, String> collect(LogicalPlan plan, Map<NameId, FieldSemantics> fields) {
        Map<NameId, Unit> units = new HashMap<>();
        fields.forEach((id, field) -> {
            // Indices that disagree on the unit report a comma-separated list: there is no single unit to propagate.
            if (field.unit() != null && field.unit().contains(",") == false) {
                units.put(id, Unit.parse(field.unit()));
            }
        });
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                Unit unit = unitOf(alias.child(), units);
                if (unit != null) {
                    units.put(alias.id(), unit);
                }
            });
            if (node instanceof MergePlan merge) {
                ColumnSemantics.mergeBranches(merge, units);
            }
        });
        Map<NameId, String> formatted = new HashMap<>();
        units.forEach((id, unit) -> {
            // A field keeps the unit as its mapping spells it, as in METRICS_INFO; derived units use normalized names.
            FieldSemantics field = fields.get(id);
            formatted.put(id, field != null && field.unit() != null ? field.unit() : unit.format());
        });
        return formatted;
    }

    @Nullable
    static Unit unitOf(@Nullable Expression e, Map<NameId, Unit> units) {
        if (e instanceof Attribute attribute) {
            return units.get(attribute.id());
        }
        if (e instanceof UnitAware unitAware) {
            return unitAware.unit(child -> unitOf(child, units));
        }
        return null;
    }
}
