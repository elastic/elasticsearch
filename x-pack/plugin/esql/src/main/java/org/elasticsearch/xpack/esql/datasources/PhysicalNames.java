/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for the declared-schema logical&rarr;physical column rename ({@code source}).
 *
 * <p>The whole external-read stack — plan, reconciliation ({@link SchemaReconciliation} / {@link ColumnMapping} /
 * {@link FilterAdaptation}), projection intersection — works in <b>logical</b> names. A {@code source} rename is
 * applied only at the last mile, on the names handed to a format reader or its pushdown SPI, so readers are fully
 * rename-agnostic and reconciliation is never disturbed. Every reader-facing name surface (projection, read schema,
 * pushed filter, deferred-extraction columns, aggregate-stats refs) routes its names through this class.
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
     * Invariant guard: assert that no <i>logical</i> rename-source name has survived into a reader-facing name set —
     * i.e. every such surface was physicalized. A leaked logical name would make a reader look up a column the file
     * does not have (wrong/null column, hard "missing" failure, or a silently mis-pushed predicate). Call at each mint
     * boundary (read-context build, {@code pushFilters} input, extraction/stat column arrays) so a surface that forgets
     * to translate trips loudly in tests rather than returning wrong rows in production. Cheap set-membership scan under
     * {@code assert}; no cost when assertions are off.
     */
    public static boolean noLogicalNamesRemain(Collection<String> readerFacingNames, Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || readerFacingNames == null) {
            return true;
        }
        for (String name : readerFacingNames) {
            if (renames.containsKey(name)) {
                return false;
            }
        }
        return true;
    }
}
