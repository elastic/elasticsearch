/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for the declared-schema logical&rarr;physical column rename ({@code path}).
 *
 * <p>The whole external-read stack — plan, reconciliation ({@link SchemaReconciliation} / {@link ColumnMapping} /
 * {@link FilterAdaptation}), projection intersection — works in <b>logical</b> names. A {@code path} rename is
 * applied only at the last mile, on the names handed to a format reader or its pushdown SPI, so readers are fully
 * rename-agnostic and reconciliation is never disturbed. Every reader-facing name surface (projection, read schema,
 * pushed filter, TopN threshold, aggregate-stats refs, deferred field-extraction column names) routes its names through
 * this class — including the deferred columns, which are extracted from the file by name after the eager projection and
 * so need the same physicalization ({@code LocalExecutionPlanner#planExternalFieldExtract}).
 *
 * <p>Names are treated as <b>opaque whole strings</b>: a dotted name ({@code a.b.c}) is a single flat column name in
 * this model and is never split here (a reader that navigates dots does so on the physical name it receives).
 */
public final class PhysicalNames {

    private PhysicalNames() {}

    /** The physical (file) name for a logical column, or the name unchanged when it is not renamed / no renames apply. */
    public static String translate(String logicalName, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || logicalName == null) {
            return logicalName;
        }
        return renames.getOrDefault(logicalName, logicalName);
    }

    /** Physicalize a list of projected column names (order and cardinality preserved). Returns the input if no renames. */
    public static List<String> translateNames(List<String> logicalNames, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || logicalNames == null || logicalNames.isEmpty()) {
            return logicalNames;
        }
        List<String> out = new ArrayList<>(logicalNames.size());
        for (String name : logicalNames) {
            out.add(translate(name, renames));
        }
        return out;
    }

    /**
     * Physicalize a read schema, renaming each attribute to its physical name via {@link Attribute#withName(String)}
     * (type and attribute kind preserved). Non-renamed attributes are returned as-is. Returns the input if no renames.
     */
    public static List<Attribute> translateSchema(List<Attribute> logicalSchema, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || logicalSchema == null || logicalSchema.isEmpty()) {
            return logicalSchema;
        }
        List<Attribute> out = new ArrayList<>(logicalSchema.size());
        for (Attribute attr : logicalSchema) {
            String physical = translate(attr.name(), renames);
            out.add(physical.equals(attr.name()) ? attr : attr.withName(physical));
        }
        return out;
    }

    /**
     * Rewrite the column names referenced by each expression through {@code renames} (via {@link Attribute#withName},
     * which preserves the attribute's {@link org.elasticsearch.xpack.esql.core.expression.NameId} — so a physical&rarr;
     * logical round-trip restores the original attribute identity). Used to physicalize the conjuncts handed to a
     * format's filter-pushdown mint (the opaque predicate then references file columns), and — with {@link #inverse} —
     * to map the mint's returned pushed/remainder expressions back to logical for the plan and reconciliation.
     */
    public static List<Expression> translateExpressionNames(List<Expression> expressions, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || expressions == null || expressions.isEmpty()) {
            return expressions;
        }
        List<Expression> out = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            out.add(expression.transformDown(Attribute.class, a -> {
                String physical = translate(a.name(), renames);
                return physical.equals(a.name()) ? a : a.withName(physical);
            }));
        }
        return out;
    }

    /** The physical&rarr;logical inverse of a logical&rarr;physical rename map (renames are 1:1, so the inverse is well-defined). */
    public static Map<String, String> inverse(Map<String, String> renames) {
        if (renames == null || renames.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new HashMap<>(renames.size());
        for (Map.Entry<String, String> e : renames.entrySet()) {
            out.put(e.getValue(), e.getKey());
        }
        return Map.copyOf(out);
    }

    /**
     * Invariant guard: {@code true} iff no <i>logical</i> rename-source name survives in a reader-facing name set — i.e.
     * the surface was physicalized. A leaked logical name would make a reader look up a column the file does not have
     * (a silently mis-pushed predicate on the pushdown path). Wired as an {@code assert} at the pushed-filter mint
     * ({@code PushFiltersToSource}), the correctness-critical surface where a mistranslation is silent; the projection
     * and aggregate surfaces are covered instead by the per-format rename ITs. Cheap set-membership scan; no cost with
     * assertions off.
     */
    public static boolean noLogicalNamesRemain(Collection<String> readerFacingNames, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || readerFacingNames == null) {
            return true;
        }
        for (String name : readerFacingNames) {
            // A name that is a rename KEY but NOT also a rename VALUE is an untranslated logical name — translation was
            // skipped. A name that is BOTH a key and a value is a swap/chain physical target (e.g. {a->b, b->a}: the
            // correctly-translated physical `b` is also logical `b`'s key): the logical and physical name spaces overlap
            // there, so a key that is also a value is legitimately reader-facing, not a translation miss.
            if (renames.containsKey(name) && renames.containsValue(name) == false) {
                return false;
            }
        }
        return true;
    }
}
