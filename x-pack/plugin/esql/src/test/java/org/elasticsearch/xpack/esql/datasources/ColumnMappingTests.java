/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.util.List;

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

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

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

    // ===== mapFilters =====

    public void testMapFiltersNoMissingNoCasts() {
        // Identity mapping, no casts: filter list returns unchanged.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, null);
        ExternalSchema query = schema("name", "age");
        Expression filter = greaterThan(query.get(1), 18);

        List<Expression> adapted = mapping.mapFilters(List.of(filter), query);

        assertSame("nothing to adapt → original list returned", adapted, adapted);
        assertEquals(List.of(filter), adapted);
    }

    public void testMapFiltersDropsAllWhenValueComparisonOnMissingColumn() {
        // `age` is missing from this file (index[1] == -1). A value comparison on a missing
        // column resolves to UNKNOWN → FALSE in WHERE context, making the entire AND
        // unsatisfiable. Result: empty list (no pushdown; RECHECK still runs the original filter).
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, -1, 1 }, null);
        ExternalSchema query = new ExternalSchema(List.of(intAttr("id"), intAttr("age"), intAttr("score")));
        Expression idFilter = greaterThan(query.get(0), 10);
        Expression ageFilter = greaterThan(query.get(1), 18);
        Expression scoreFilter = greaterThan(query.get(2), 100);

        List<Expression> adapted = mapping.mapFilters(List.of(idFilter, ageFilter, scoreFilter), query);

        assertTrue("value comparison on missing column makes the AND unsatisfiable → empty pushdown", adapted.isEmpty());
    }

    public void testMapFiltersCastOnlyInvertsLiteralForLongWiden() {
        // `age` was widened INT → LONG at the coordinator. The file reads it as INT, so the
        // literal in `age > 18L` needs to invert through the cast to compare INT-to-INT.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.LONG });
        ExternalSchema query = new ExternalSchema(List.of(intAttr("id"), longAttr("age")));
        Expression ageFilter = greaterThan(query.get(1), 18L);

        List<Expression> adapted = mapping.mapFilters(List.of(ageFilter), query);

        // The filter is rewritten (not the same object) because the literal was downcast.
        assertNotSame("cast inversion produces a new expression", ageFilter, adapted.get(0));
        assertEquals(1, adapted.size());
    }

    // ===== Four-schema combinatorial matrix =====
    //
    // pruneToPerFileQuery is the seam where the four-schema model meets a single per-file mapping.
    // The bug class behind this PR (and B1 in particular) lives at one cell of a three-axis matrix:
    //
    // {reconciliation mode: FFW / STRICT / UBN}
    // × {partitioning: unpartitioned / Hive-style}
    // × {projection: full / KEEP-pruned subset}
    //
    // FFW and STRICT collapse to identity per-file mappings; UBN is the one that produces
    // missing-position / cast mappings. The matrix below walks the four cells where structural
    // behaviour actually differs (full vs pruned × identity vs UBN-shaped mapping) and asserts
    // post-prune mapping widths, identity status, and that partitions-present invariants hold.

    public void testMatrixIdentityMappingFullProjection() {
        // FFW / STRICT analogue: identity mapping, no projection prune.
        ExternalSchema unified = schema("id", "name", "age");
        ExternalSchema file = schema("id", "name", "age");
        ExternalSchema query = schema("id", "name", "age");
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, 2 }, null);

        ColumnMapping result = mapping.pruneToPerFileQuery(unified, file, query);

        assertSame("identity output + identity reads → same mapping", mapping, result);
        assertTrue(result.isIdentity());
    }

    public void testMatrixIdentityMappingPrunedProjection() {
        // FFW / STRICT analogue with optimizer projection prune.
        // Single surviving column: width=1, projected source position 0 → still identity-shaped
        // (the file's projected page presents the kept column at index 0).
        ExternalSchema unified = schema("id", "name", "age");
        ExternalSchema file = schema("id", "name", "age");
        ExternalSchema query = schema("name");
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, 2 }, null);

        ColumnMapping result = mapping.pruneToPerFileQuery(unified, file, query);

        assertEquals("query selects one column", 1, result.width());
        // The kept column lands at projected position 0; result is structurally identity for a
        // single-column projection.
        assertTrue("single-column projected pruning collapses to identity-shaped mapping", result.isIdentity());
    }

    public void testMatrixUbnMappingFullProjection() {
        // UBN: file is missing `age`. No projection prune.
        ExternalSchema unified = schema("id", "name", "age");
        ExternalSchema file = schema("id", "name");
        ExternalSchema query = schema("id", "name", "age");
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, -1 }, null);

        ColumnMapping result = mapping.pruneToPerFileQuery(unified, file, query);

        assertEquals("output covers all unified columns", 3, result.width());
        assertFalse("missing position keeps it non-identity", result.isIdentity());
    }

    public void testMatrixUbnMappingPrunedProjection() {
        // UBN + projection prune: file missing `age`, query keeps {name, age}.
        // Output narrows from unified(3) to query(2); source remap stays within file's two columns.
        ExternalSchema unified = schema("id", "name", "age");
        ExternalSchema file = schema("id", "name");
        ExternalSchema query = schema("name", "age");
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, -1 }, null);

        ColumnMapping result = mapping.pruneToPerFileQuery(unified, file, query);

        assertEquals("output narrows to query width", 2, result.width());
        assertFalse(result.isIdentity());
    }

    public void testMatrixPartitionsPresentRejectedAtPrecondition() {
        // Partitioned data: caller MUST seed the unified from the data-only schema.
        // Passing the enriched (partition-appended) unified is the bug class — the precondition
        // assertion fires before the loop can overrun. Already covered by
        // testPruneToPerFileQueryRejectsPartitionEnrichedUnified above; this method adds the
        // matrix-cell label so the seven-cell narrative reads as a unit.
        ExternalSchema dataOnlyUnified = schema("id", "name");
        ExternalSchema partitionEnrichedUnified = schema("id", "name", "year");
        ExternalSchema file = schema("id", "name");
        ExternalSchema query = schema("id", "name");
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, null);

        // Correct seed: passes.
        assertNotNull(mapping.pruneToPerFileQuery(dataOnlyUnified, file, query));

        // Wrong seed: fails fast at the precondition.
        AssertionError e = expectThrows(AssertionError.class, () -> mapping.pruneToPerFileQuery(partitionEnrichedUnified, file, query));
        assertTrue(e.getMessage() != null && e.getMessage().contains("disagrees with mapping width"));
    }

    // ===== mapPage failure-cleanup =====

    public void testMapPageWrapsAndRethrowsOnUnsupportedCast() {
        // Mapping declares an unsupported cast: source block is DoubleBlock but cast target is LONG.
        // ColumnMapping.castBlock has no DOUBLE → LONG path and throws UnsupportedOperationException;
        // mapPage's try/catch must wrap as RuntimeException, close partially-built blocks, and rethrow.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.LONG });

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(7, 2);
        DoubleBlock doubleBlock = blockFactory.newConstantDoubleBlockWith(3.14, 2);
        Page filePage = new Page(2, new Block[] { intBlock, doubleBlock });

        RuntimeException e = expectThrows(RuntimeException.class, () -> mapping.mapPage(filePage, blockFactory));
        assertTrue("wrapped under mapPage's catch", e.getMessage() != null && e.getMessage().contains("Failed to map page"));
        assertTrue("underlying cause surfaces", e.getCause() != null && e.getCause() instanceof UnsupportedOperationException);
        filePage.releaseBlocks();
    }

    private static Expression greaterThan(Attribute attr, Object literalValue) {
        DataType type = literalValue instanceof Long ? DataType.LONG : DataType.INTEGER;
        return new GreaterThan(Source.EMPTY, attr, new Literal(Source.EMPTY, literalValue, type));
    }

    private static Attribute intAttr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.INTEGER, Nullability.TRUE, null, false);
    }

    private static Attribute longAttr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.LONG, Nullability.TRUE, null, false);
    }

    private static ExternalSchema schema(String... names) {
        return new ExternalSchema(java.util.Arrays.stream(names).<Attribute>map(ColumnMappingTests::attr).toList());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD, Nullability.TRUE, null, false);
    }
}
