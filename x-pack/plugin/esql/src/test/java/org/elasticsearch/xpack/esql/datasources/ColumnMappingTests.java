/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Direct unit tests for {@link ColumnMapping#pruneToPerFileQuery} — the fused method that
 * narrows a per-file mapping from (Unified width, file-natural source positions) to
 * (Query width, projected-page source positions). Covers the targeted shapes:
 * <ul>
 *   <li>{@code kept=0} — the query selects nothing from this file
 *   <li>cast-only — no missing columns, only widening casts
 *   <li>missing-only — some unified columns absent from the file, no casts
 *   <li>partitions-present — regression guard for the partition-column seed bug
 *       (an enriched Unified passed alongside a data-only-sized mapping would
 *       overrun {@code index.length}; the precondition assertion catches the mismatch)
 * </ul>
 */
public class ColumnMappingTests extends ESTestCase {

    public void testPruneToPerFileQueryKeptZero() {
        // Unified [name, age, city]; file [name, age, city]; query selects nothing reachable.
        ExternalSchema unified = schema("name", "age", "city");
        ExternalSchema file = schema("name", "age", "city");
        ExternalSchema query = schema("salary"); // none of unified

        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, 2 }, null);

        ColumnMapping pruned = mapping.pruneToPerFileQuery(unified, file, query);

        assertEquals("kept=0 produces an empty-width mapping", 0, pruned.width());
        assertTrue("kept=0 is trivially identity-shaped", pruned.isIdentity());
    }

    public void testPruneToPerFileQueryCastOnly() {
        // No missing columns, but `age` widens from INT to LONG.
        ExternalSchema unified = schema("name", "age");
        ExternalSchema file = schema("name", "age");
        ExternalSchema query = schema("name", "age");

        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.LONG });

        ColumnMapping pruned = mapping.pruneToPerFileQuery(unified, file, query);

        // No prune, no remap; mapping is returned as-is because output covers unified and file is read whole.
        assertSame("identity output + identity reads returns this", mapping, pruned);
        assertFalse("cast-only is not identity", pruned.isIdentity());
        assertEquals(2, pruned.width());
    }

    public void testPruneToPerFileQueryMissingOnly() {
        // Unified has [name, age, city]; this file only has [name, city].
        // Query wants all three; partial coverage produces a mapping with one missing position.
        ExternalSchema unified = schema("name", "age", "city");
        ExternalSchema file = schema("name", "city");
        ExternalSchema query = schema("name", "age", "city");

        // Mapping built by SchemaReconciliation for this file:
        // unified[0]=name → file[0], unified[1]=age → missing (-1), unified[2]=city → file[1].
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, -1, 1 }, null);

        ColumnMapping pruned = mapping.pruneToPerFileQuery(unified, file, query);

        // File is read whole (both columns survive projection), so source indices stay at 0 and 1.
        assertEquals("output covers all unified columns", 3, pruned.width());
        assertFalse("missing-only is not identity (has missing positions)", pruned.isIdentity());
    }

    /**
     * Regression guard for the partition-column seed bug. If the caller mistakenly seeds
     * {@code unifiedSchema} from the post-enrichment schema (which appends partition attributes)
     * while {@code ColumnMapping.index} was sized against the data-only unified, the loop in
     * {@code pruneToPerFileQuery} would walk {@code i < unifiedSchema.size()} and trip on
     * {@code index[i]} for {@code i >= index.length}. The width precondition catches the
     * mismatch up front with a clear message.
     */
    public void testPruneToPerFileQueryRejectsPartitionEnrichedUnified() {
        // ColumnMapping sized to data-only unified (2 columns).
        ExternalSchema dataOnlyUnified = schema("name", "age");
        ExternalSchema enrichedUnified = schema("name", "age", "year"); // year is the partition column appended
        ExternalSchema file = schema("name", "age");
        ExternalSchema query = schema("name", "age", "year");

        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, null);

        // Data-only Unified matches the mapping — no overrun.
        ColumnMapping pruned = mapping.pruneToPerFileQuery(dataOnlyUnified, file, query);
        assertNotNull(pruned);

        // Enriched Unified is wider than the mapping — precondition assertion fires (or, if assertions
        // are off, the original ArrayIndexOutOfBoundsException is what the precondition exists to prevent).
        AssertionError e = expectThrows(AssertionError.class, () -> mapping.pruneToPerFileQuery(enrichedUnified, file, query));
        assertTrue(
            "assertion message points at the width mismatch",
            e.getMessage() != null && e.getMessage().contains("disagrees with mapping width")
        );
    }

    private static ExternalSchema schema(String... names) {
        return new ExternalSchema(java.util.Arrays.stream(names).<Attribute>map(ColumnMappingTests::attr).toList());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD, Nullability.TRUE, null, false);
    }
}
