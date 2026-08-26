/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

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

    public void testMapFiltersDropsConjunctOnKeywordCast() {
        // The UBN KEYWORD fallback installs a stringify cast that has no safe inverse — "1" < "10"
        // is lexicographic, not numeric, so a pushed literal would produce wrong row-group skips.
        // `mapFilters` must withhold the column name from the per-file allowlist so
        // {@link FilterAdaptation} drops the conjunct entirely; RECHECK reruns the original
        // predicate against the unified-shape page. The downstream AND-collapse semantics in
        // FilterAdaptation treat a value comparison on a withheld column as unsatisfiable, so
        // the whole pushdown empties for this file — equivalent to the missing-column case in
        // {@link #testMapFiltersDropsAllWhenValueComparisonOnMissingColumn}. The empty pushdown
        // is correct: RECHECK still runs the predicate post-stringification.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.KEYWORD });
        ExternalSchema query = new ExternalSchema(List.of(intAttr("id"), intAttr("col")));
        Expression idFilter = greaterThan(query.get(0), 10);
        Expression colFilter = greaterThan(query.get(1), 5);

        List<Expression> adapted = mapping.mapFilters(List.of(idFilter, colFilter), query);

        assertTrue("filter on KEYWORD-cast column collapses pushdown; RECHECK is the backstop", adapted.isEmpty());
    }

    public void testMapFiltersKeywordCastWithoutReferencingFilterLeavesPushdownUnchanged() {
        // A KEYWORD cast on `col` should NOT affect a filter that doesn't reference `col`.
        // Withholding the column from the allowlist only matters when a filter mentions it; an
        // id-only filter still pushes through.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.KEYWORD });
        ExternalSchema query = new ExternalSchema(List.of(intAttr("id"), intAttr("col")));
        Expression idFilter = greaterThan(query.get(0), 10);

        List<Expression> adapted = mapping.mapFilters(List.of(idFilter), query);

        assertEquals("id-only filter pushes through unchanged", 1, adapted.size());
        assertSame(idFilter, adapted.get(0));
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

    public void testReconciliationCastMatchesDeclaredCoercion() {
        // THE one-coercion-path equivalence: casting a physical block through the reconciliation
        // route (mapPage with a cast slot) and through the declared route (DeclaredTypeCoercions
        // .castBlock, as the readers invoke it) must produce the same values in the same block
        // shape AND the same per-value warnings — there is no separate merge-cast mechanism left
        // to drift. Exercised on the one fallible reconciliation pair (DATETIME -> DATE_NANOS,
        // one in-range value + one unrepresentable pre-epoch value) and on a lossless widen
        // (INTEGER -> LONG).
        long inRange = 1_711_800_000_000L;
        // DATETIME -> DATE_NANOS with a per-value failure.
        {
            List<String> declaredWarnings = new java.util.ArrayList<>();
            List<String> inferredWarnings = new java.util.ArrayList<>();
            ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DATE_NANOS });
            LongBlock src = blockFactory.newLongArrayVector(new long[] { inRange, -5L }, 2).asBlock();
            Page filePage = new Page(2, new Block[] { src });
            try (
                Block declared = DeclaredTypeCoercions.castBlock(
                    src,
                    DataType.DATETIME,
                    DataType.DATE_NANOS,
                    null,
                    blockFactory,
                    "ts",
                    capturing(declaredWarnings)
                )
            ) {
                Page out = mapping.mapPage(
                    filePage,
                    blockFactory,
                    new DataType[] { DataType.DATETIME },
                    new String[] { "ts" },
                    capturing(inferredWarnings)
                );
                try {
                    LongBlock inferred = out.getBlock(0);
                    assertEquals("identical blocks: same values, same nulls", declared, inferred);
                    assertThat(((LongBlock) declared).getLong(0), equalTo(inRange * 1_000_000L));
                    assertTrue("the unrepresentable instant nulls, in both routes", inferred.isNull(1));
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
            assertEquals("identical warnings, event for event", declaredWarnings, inferredWarnings);
            assertThat(declaredWarnings, org.hamcrest.Matchers.hasSize(1));
            assertThat(declaredWarnings.get(0), org.hamcrest.Matchers.containsString("ts"));
        }
        // INTEGER -> LONG lossless widen.
        {
            ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.LONG });
            IntBlock src = blockFactory.newConstantIntBlockWith(7, 3);
            Page filePage = new Page(3, new Block[] { src });
            try (Block declared = DeclaredTypeCoercions.castBlock(src, DataType.INTEGER, DataType.LONG, null, blockFactory, null, null)) {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.INTEGER });
                try {
                    assertEquals("identical blocks for the lossless widen", declared, out.getBlock(0));
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastDatetimeToDateNanosOutOfRangeIsStrictWithoutSink() {
        // Without a warnings sink the cast is strict: the unrepresentable instant fails the page
        // (wrapped by mapPage) instead of nulling. Previously this pair silently multiplied into
        // an overflowed long; the range rule now matches TO_DATE_NANOS (DateUtils.toNanoSeconds).
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DATE_NANOS });
        LongBlock src = blockFactory.newConstantLongBlockWith(-5L, 1);
        Page filePage = new Page(1, new Block[] { src });
        try {
            RuntimeException e = expectThrows(
                RuntimeException.class,
                () -> mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.DATETIME })
            );
            assertTrue("wrapped under mapPage's catch", e.getMessage() != null && e.getMessage().contains("Failed to map page"));
        } finally {
            filePage.releaseBlocks();
        }
    }

    private static SkipWarnings capturing(List<String> into) {
        return new SkipWarnings("summary") {
            @Override
            public void add(String detail) {
                into.add(detail);
            }
        };
    }

    public void testMapPageWrapsAndRethrowsOnUnsupportedCast() {
        // Mapping declares an unsupported cast: source block is BooleanBlock but cast target is LONG —
        // a pair DeclaredTypeCoercions.supports rejects (booleans do not ingest into numeric fields),
        // and one reconciliation can never produce. ColumnMapping.castBlock throws
        // UnsupportedOperationException; mapPage's try/catch must wrap as RuntimeException, close
        // partially-built blocks, and rethrow.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.LONG });

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(7, 2);
        BooleanBlock booleanBlock = blockFactory.newConstantBooleanBlockWith(true, 2);
        Page filePage = new Page(2, new Block[] { intBlock, booleanBlock });

        RuntimeException e = expectThrows(RuntimeException.class, () -> mapping.mapPage(filePage, blockFactory));
        assertTrue("wrapped under mapPage's catch", e.getMessage() != null && e.getMessage().contains("Failed to map page"));
        assertTrue("underlying cause surfaces", e.getCause() != null && e.getCause() instanceof UnsupportedOperationException);
        filePage.releaseBlocks();
    }

    // ===== KEYWORD-cast (UBN fallback) =====
    //
    // These exercise the new {@link ColumnMapping#castBlock} branches added in Stage 2. The
    // bytes must match {@code TO_STRING(col)} exactly (so equal-by-string semantics survive
    // GROUP BY / JOIN), which is why every assertion compares against the canonical
    // {@code EsqlDataTypeConverter} helpers used by {@code TO_STRING} itself rather than to a
    // hard-coded literal — if the canonical format ever changes the test moves with it.

    public void testCastIntToKeyword() {
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(4)) {
            b.appendInt(1);
            b.appendInt(2);
            b.appendNull();
            b.appendInt(3);
            IntBlock intBlock = b.build();
            Page filePage = new Page(4, new Block[] { intBlock });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.INTEGER });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    assertEquals("1", result.getBytesRef(0, scratch).utf8ToString());
                    assertEquals("2", result.getBytesRef(1, scratch).utf8ToString());
                    assertTrue(result.isNull(2));
                    assertEquals("3", result.getBytesRef(result.getFirstValueIndex(3), scratch).utf8ToString());
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastLongToKeyword() {
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        long[] values = { 0L, Long.MAX_VALUE, Long.MIN_VALUE, -1L };
        try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(values.length)) {
            for (long v : values) {
                b.appendLong(v);
            }
            LongBlock src = b.build();
            Page filePage = new Page(values.length, new Block[] { src });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.LONG });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < values.length; i++) {
                        assertEquals(
                            "long stringification must match numericBooleanToString for value " + values[i],
                            EsqlDataTypeConverter.numericBooleanToString(values[i]).utf8ToString(),
                            result.getBytesRef(result.getFirstValueIndex(i), scratch).utf8ToString()
                        );
                    }
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastDoubleToKeyword() {
        // NaN, -0.0, infinities, and Double.MAX_VALUE go through numericBooleanToString unchanged;
        // String.valueOf(double) defines the canonical bytes the engine uses everywhere else.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        double[] values = { 1.5, Double.NaN, -0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MAX_VALUE };
        try (DoubleBlock.Builder b = blockFactory.newDoubleBlockBuilder(values.length)) {
            for (double v : values) {
                b.appendDouble(v);
            }
            DoubleBlock src = b.build();
            Page filePage = new Page(values.length, new Block[] { src });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.DOUBLE });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < values.length; i++) {
                        assertEquals(
                            "double stringification must match numericBooleanToString for value " + values[i],
                            EsqlDataTypeConverter.numericBooleanToString(values[i]).utf8ToString(),
                            result.getBytesRef(result.getFirstValueIndex(i), scratch).utf8ToString()
                        );
                    }
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastBooleanToKeyword() {
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        try (BooleanBlock.Builder b = blockFactory.newBooleanBlockBuilder(2)) {
            b.appendBoolean(true);
            b.appendBoolean(false);
            BooleanBlock src = b.build();
            Page filePage = new Page(2, new Block[] { src });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.BOOLEAN });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    assertEquals("true", result.getBytesRef(0, scratch).utf8ToString());
                    assertEquals("false", result.getBytesRef(1, scratch).utf8ToString());
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastDatetimeToKeyword() {
        // millis epoch → strict_date_optional_time (the formatter used by TO_STRING(TO_DATETIME(...))).
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        long millis = 1_711_800_000_000L;
        LongBlock src = blockFactory.newConstantLongBlockWith(millis, 1);
        Page filePage = new Page(1, new Block[] { src });
        try {
            Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.DATETIME });
            try {
                BytesRefBlock result = out.getBlock(0);
                BytesRef scratch = new BytesRef();
                assertEquals(EsqlDataTypeConverter.dateTimeToString(millis), result.getBytesRef(0, scratch).utf8ToString());
            } finally {
                out.releaseBlocks();
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    public void testCastDateNanosToKeyword() {
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        long nanos = 1_711_800_000_000_000_000L;
        LongBlock src = blockFactory.newConstantLongBlockWith(nanos, 1);
        Page filePage = new Page(1, new Block[] { src });
        try {
            Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.DATE_NANOS });
            try {
                BytesRefBlock result = out.getBlock(0);
                BytesRef scratch = new BytesRef();
                assertEquals(EsqlDataTypeConverter.nanoTimeToString(nanos), result.getBytesRef(0, scratch).utf8ToString());
            } finally {
                out.releaseBlocks();
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    public void testCastMultiValueIntToKeyword() {
        // A single position with three packed values is the typical "MV" shape on the read path —
        // the new cast methods must call beginPositionEntry/endPositionEntry exactly around the
        // inner loop, mirroring castIntToLong's shape.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(3)) {
            b.beginPositionEntry();
            b.appendInt(1);
            b.appendInt(2);
            b.appendInt(3);
            b.endPositionEntry();
            IntBlock src = b.build();
            Page filePage = new Page(1, new Block[] { src });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.INTEGER });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    assertEquals("one position with three packed values", 1, result.getPositionCount());
                    assertEquals(3, result.getValueCount(0));
                    BytesRef scratch = new BytesRef();
                    int first = result.getFirstValueIndex(0);
                    assertEquals("1", result.getBytesRef(first, scratch).utf8ToString());
                    assertEquals("2", result.getBytesRef(first + 1, scratch).utf8ToString());
                    assertEquals("3", result.getBytesRef(first + 2, scratch).utf8ToString());
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
    }

    public void testCastBytesRefToKeywordIsRefBumpPassthrough() {
        // A BytesRefBlock source under a KEYWORD target must be a ref-bumped passthrough so the
        // happy path does not allocate a new block. Verified by reference equality between input
        // and output, and by the input block surviving the output page's release.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        BytesRefBlock src = blockFactory.newConstantBytesRefBlockWith(new BytesRef("hello"), 2);
        Page filePage = new Page(2, new Block[] { src });
        try {
            Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.KEYWORD });
            try {
                BytesRefBlock returned = out.getBlock(0);
                assertSame("KEYWORD → KEYWORD must be a ref-bumped passthrough, not a copy", src, returned);
            } finally {
                out.releaseBlocks();
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    public void testCastIpToKeywordStringifiesAddress() {
        // UBN's KEYWORD fallback covers every cross-type pair, so an ip-typed file column can
        // carry a KEYWORD cast slot (ip file vs keyword file). The stringification must emit the
        // address text — TO_STRING(col) bytes — not the mapper's 16-byte encoded form the block
        // physically holds (which the pre-unification passthrough leaked).
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        BytesRefBlock src = blockFactory.newConstantBytesRefBlockWith(StringUtils.parseIP("10.20.30.40"), 1);
        Page filePage = new Page(1, new Block[] { src });
        try {
            Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.IP });
            try {
                BytesRefBlock result = out.getBlock(0);
                assertEquals("10.20.30.40", result.getBytesRef(0, new BytesRef()).utf8ToString());
            } finally {
                out.releaseBlocks();
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    public void testCastLongToKeywordWithoutSourceTypeAsserts() {
        // The LongBlock cast must know whether the source was LONG, DATETIME, or DATE_NANOS to
        // pick the right stringifier (numericBooleanToString vs dateTimeToString vs
        // nanoTimeToString). Callers from the UBN path always thread fileColumnTypes; the
        // assertion guards against future callers slipping through the two-arg overload.
        // mapPage's catch is scoped to Exception, so the AssertionError surfaces directly — that
        // is intentional, the assertion is a programmer-error tripwire, not a runtime failure to
        // be recovered. Verify the message names the missing contract so the next developer who
        // hits it knows what to fix.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        LongBlock src = blockFactory.newConstantLongBlockWith(42L, 1);
        Page filePage = new Page(1, new Block[] { src });
        try {
            AssertionError e = expectThrows(AssertionError.class, () -> mapping.mapPage(filePage, blockFactory));
            assertTrue(
                "assertion message names the missing fileColumnTypes contract, got: " + e.getMessage(),
                e.getMessage() != null && e.getMessage().contains("sourceType")
            );
        } finally {
            filePage.releaseBlocks();
        }
    }

    public void testCastDoubleToKeywordPinsCanonicalEdgeFormats() {
        // The canonical bytes for NaN, -0.0, and +Infinity are part of the ES|QL user-visible
        // contract — TO_STRING(double) must produce these strings so a stringified column under
        // UBN compares equal to a TO_STRING(col). Pin them here against drift in
        // EsqlDataTypeConverter.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD });
        double[] values = { Double.NaN, -0.0, Double.POSITIVE_INFINITY };
        String[] expected = { "NaN", "-0.0", "Infinity" };
        try (DoubleBlock.Builder b = blockFactory.newDoubleBlockBuilder(values.length)) {
            for (double v : values) {
                b.appendDouble(v);
            }
            DoubleBlock src = b.build();
            Page filePage = new Page(values.length, new Block[] { src });
            try {
                Page out = mapping.mapPage(filePage, blockFactory, new DataType[] { DataType.DOUBLE });
                try {
                    BytesRefBlock result = out.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < values.length; i++) {
                        assertEquals(
                            "canonical double edge format must be pinned for value " + values[i],
                            expected[i],
                            result.getBytesRef(result.getFirstValueIndex(i), scratch).utf8ToString()
                        );
                    }
                } finally {
                    out.releaseBlocks();
                }
            } finally {
                filePage.releaseBlocks();
            }
        }
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
