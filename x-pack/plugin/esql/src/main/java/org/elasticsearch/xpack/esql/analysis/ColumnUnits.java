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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Derives the unit of every column of a plan from the {@code meta.unit} of the mapped fields it reads.
 * <p>
 * Each expression owns its rule through {@link UnitAware}; an expression without one has an unknown unit,
 * and so does everything computed from it.
 */
public final class ColumnUnits {

    private ColumnUnits() {}

    /**
     * Returns the unit of every attribute of {@code plan} whose unit is known, keyed by attribute id, or every unit, sorted,
     * when its sources disagree.
     *
     * @param fields the semantics of the mapped fields, see {@link ColumnSemantics#collect}
     */
    public static Map<NameId, List<String>> collect(LogicalPlan plan, Map<NameId, FieldSemantics> fields) {
        Map<NameId, Unit> units = new HashMap<>();
        // Columns whose sources disagree on the unit list every unit. There is no single unit to compute with, so the
        // list follows renames and merges but nothing computed from the column has a unit.
        Map<NameId, SortedSet<String>> listed = new HashMap<>();
        fields.forEach((id, field) -> {
            if (field.units().size() == 1) {
                units.put(id, Unit.parse(field.units().first()));
            } else if (field.units().size() > 1) {
                listed.put(id, field.units());
            }
        });
        plan.forEachUp(node -> {
            node.forEachExpression(Alias.class, alias -> {
                if (alias.child() instanceof Attribute source && listed.containsKey(source.id())) {
                    listed.put(alias.id(), listed.get(source.id()));
                    return;
                }
                Unit unit = unitOf(alias.child(), units);
                if (unit != null) {
                    units.put(alias.id(), unit);
                }
            });
            if (node instanceof MergePlan merge) {
                ColumnSemantics.forEachMergedColumn(merge, (column, sources) -> mergeUnits(column, sources, fields, units, listed));
            }
        });
        Map<NameId, List<String>> formatted = new HashMap<>();
        units.forEach((id, unit) -> formatted.put(id, List.of(spelling(id, unit, fields))));
        listed.forEach((id, list) -> formatted.put(id, List.copyOf(list)));
        return formatted;
    }

    /**
     * A merged column has the unit of its branches when they agree, and lists every unit when they do not.
     * It has none when a branch that has the column does not know its unit.
     */
    private static void mergeUnits(
        Attribute column,
        List<NameId> sources,
        Map<NameId, FieldSemantics> fields,
        Map<NameId, Unit> units,
        Map<NameId, SortedSet<String>> listed
    ) {
        Unit agreed = null;
        boolean agree = true;
        SortedSet<String> all = new TreeSet<>();
        for (NameId source : sources) {
            Unit unit = units.get(source);
            SortedSet<String> list = listed.get(source);
            if (list != null) {
                all.addAll(list);
                agree = false;
            } else if (unit != null) {
                all.add(spelling(source, unit, fields));
                agree &= agreed == null || agreed.equals(unit);
                agreed = unit;
            } else {
                return;
            }
        }
        if (sources.isEmpty()) {
            return;
        }
        if (agree) {
            units.put(column.id(), agreed);
            listed.remove(column.id());
        } else {
            listed.put(column.id(), all);
            units.remove(column.id());
        }
    }

    /**
     * A field keeps the unit as its mapping spells it, as in METRICS_INFO; derived units use normalized names.
     */
    private static String spelling(NameId id, Unit unit, Map<NameId, FieldSemantics> fields) {
        FieldSemantics field = fields.get(id);
        return field != null && field.units().size() == 1 ? field.units().first() : unit.format();
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
