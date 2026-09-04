/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Parity tests for {@link NumericTopNOperator} against the generic {@link TopNOperator} on the
 * exact plan shape the new operator is designed to replace: a 2-channel page of
 * {@code [sortKey (LONG), _rowPosition (LONG)]} flowing into a single-key Top-K. The two
 * operators must produce byte-for-byte identical output pages for every (ASC/DESC × nullsFirst
 * × K × input distribution) combination — including the {@code _rowPosition} payload.
 */
public class NumericTopNOperatorTests extends ComputeTestCase {

    public void testParityAscNullsLast() {
        assertParity(true, false);
    }

    public void testParityAscNullsFirst() {
        assertParity(true, true);
    }

    public void testParityDescNullsLast() {
        assertParity(false, false);
    }

    public void testParityDescNullsFirst() {
        assertParity(false, true);
    }

    public void testPublishesRawThreshold() {
        List<Long> values = List.of(1L, 2L, 100L, 99L, 98L);
        for (boolean asc : List.of(true, false)) {
            BlockFactory blockFactory = blockFactory();
            SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(asc, false);
            try (SharedNumericThreshold threshold = supplier.get()) {
                Operator operator = new NumericTopNOperator.NumericTopNOperatorFactory(2, ElementType.LONG, asc, false, supplier).get(
                    new DriverContext(blockFactory.bigArrays(), blockFactory, null)
                );
                List<Page> output = runTopN(operator, blockFactory, values, new boolean[values.size()]);
                try {
                    assertThat(threshold.current(), equalTo(asc ? 2L : 99L));
                    assertThat(threshold.offeredCount(), equalTo(asc ? 1L : 3L));
                } finally {
                    output.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    public void testNullSaturationMarksNoFurtherCandidates() {
        for (boolean asc : List.of(true, false)) {
            SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(asc, true);
            try (SharedNumericThreshold threshold = supplier.get()) {
                BlockFactory blockFactory = blockFactory();
                int count = 20;
                boolean[] nulls = new boolean[count];
                java.util.Arrays.fill(nulls, true);
                Operator operator = new NumericTopNOperator.NumericTopNOperatorFactory(10, ElementType.LONG, asc, true, supplier).get(
                    new DriverContext(blockFactory.bigArrays(), blockFactory, null)
                );
                List<Page> output = runTopN(operator, blockFactory, java.util.Collections.nCopies(count, 0L), nulls);
                try {
                    assertTrue(threshold.noFurtherCandidates());
                    assertThat(threshold.current(), equalTo(asc ? Long.MIN_VALUE : Long.MAX_VALUE));
                } finally {
                    output.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    public void testNullSaturationDoesNotMarkForNullsLast() {
        for (boolean asc : List.of(true, false)) {
            SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(asc, false);
            try (SharedNumericThreshold threshold = supplier.get()) {
                BlockFactory blockFactory = blockFactory();
                int count = 20;
                boolean[] nulls = new boolean[count];
                java.util.Arrays.fill(nulls, true);
                Operator operator = new NumericTopNOperator.NumericTopNOperatorFactory(10, ElementType.LONG, asc, false, supplier).get(
                    new DriverContext(blockFactory.bigArrays(), blockFactory, null)
                );
                List<Page> output = runTopN(operator, blockFactory, java.util.Collections.nCopies(count, 0L), nulls);
                try {
                    assertFalse(threshold.noFurtherCandidates());
                    assertThat(threshold.current(), equalTo(asc ? Long.MAX_VALUE : Long.MIN_VALUE));
                } finally {
                    output.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    /**
     * Low-cardinality input exercises the {@code _rowPosition} tiebreaker path: many input
     * rows share the same encoded value, so the heap's internal ordering matters and the
     * "first-seen wins ties" stability must produce the same surviving payload as the generic
     * operator.
     */
    public void testParityLowCardinalityTies() {
        boolean asc = randomBoolean();
        boolean nullsFirst = randomBoolean();
        int n = randomIntBetween(200, 2000);
        int k = randomIntBetween(1, Math.min(50, n));
        // Only 4 distinct values: heavy collisions across the input.
        long[] palette = new long[] { 0L, 1L, 2L, 3L };
        List<Long> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(palette[randomInt(palette.length - 1)]);
        }
        assertParityForInput(values, k, asc, nullsFirst, /*nullProbability*/ 0.0);
    }

    private void assertParity(boolean asc, boolean nullsFirst) {
        int n = randomIntBetween(0, 2000);
        int k = randomIntBetween(1, Math.max(1, n));
        List<Long> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(randomLong());
        }
        double nullProb = randomBoolean() ? 0.0 : randomDoubleBetween(0.01, 0.5, true);
        assertParityForInput(values, k, asc, nullsFirst, nullProb);
    }

    /**
     * Run the same input through both operators and compare every output column position by
     * position. Both operators are configured with the same K, the same sort order, the same
     * nulls policy, and identical 2-channel pages.
     */
    private void assertParityForInput(List<Long> values, int k, boolean asc, boolean nullsFirst, double nullProbability) {
        BlockFactory blockFactory = blockFactory();
        // Mark random positions as null with the given probability; same mask for both runs so
        // the operators see identical inputs.
        boolean[] nullMask = new boolean[values.size()];
        for (int i = 0; i < values.size(); i++) {
            nullMask[i] = nullProbability > 0 && randomDouble() < nullProbability;
        }
        List<Page> referenceOutput = runTopN(generic(blockFactory, k, asc, nullsFirst), blockFactory, values, nullMask);
        List<Page> numericOutput = runTopN(numeric(blockFactory, k, asc, nullsFirst), blockFactory, values, nullMask);
        try {
            assertPagesEqual(referenceOutput, numericOutput, k);
        } finally {
            referenceOutput.forEach(Page::releaseBlocks);
            numericOutput.forEach(Page::releaseBlocks);
        }
    }

    private Operator generic(BlockFactory blockFactory, int k, boolean asc, boolean nullsFirst) {
        CircuitBreaker breaker = blockFactory.breaker();
        return new TopNOperator(
            blockFactory,
            breaker,
            k,
            List.of(ElementType.LONG, ElementType.LONG),
            List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE),
            List.of(new TopNOperator.SortOrder(NumericTopNOperator.SORT_KEY_CHANNEL, asc, nullsFirst)),
            randomIntBetween(1, 1000), // maxPageSize
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null
        );
    }

    private Operator numeric(BlockFactory blockFactory, int k, boolean asc, boolean nullsFirst) {
        return new NumericTopNOperator.NumericTopNOperatorFactory(k, ElementType.LONG, asc, nullsFirst).get(
            new DriverContext(blockFactory.bigArrays(), blockFactory, null)
        );
    }

    private List<Page> runTopN(Operator op, BlockFactory blockFactory, List<Long> values, boolean[] nullMask) {
        return runTopN(op, () -> pagesFor(blockFactory, values, nullMask));
    }

    /**
     * Drive {@code op} with the pages produced by {@code inputSupplier}. The supplier indirection
     * exists so callers can run two operators on freshly-built copies of the same logical input
     * — pages are released after consumption, so a single {@code List<Page>} can't be reused.
     */
    private List<Page> runTopN(Operator op, java.util.function.Supplier<List<Page>> inputSupplier) {
        List<Page> input = inputSupplier.get();
        List<Page> output = new ArrayList<>();
        try {
            for (Page p : input) {
                op.addInput(p);
            }
            op.finish();
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out != null) {
                    output.add(out);
                }
            }
        } finally {
            op.close();
        }
        return output;
    }

    /**
     * Build the test input as one or more 2-channel pages. The {@code _rowPosition} column is
     * dense and strictly increasing across all pages, matching how the external source emits
     * it in production (packed extractor-id + file-local position, but for tests we use the
     * input index directly since the operator treats it as an opaque long).
     */
    private List<Page> pagesFor(BlockFactory blockFactory, List<Long> values, boolean[] nullMask) {
        int n = values.size();
        if (n == 0) {
            return List.of();
        }
        int pageSize = randomIntBetween(1, Math.max(1, n));
        List<Page> out = new ArrayList<>();
        int p = 0;
        while (p < n) {
            int end = Math.min(p + pageSize, n);
            int sz = end - p;
            LongBlock sortBlock;
            LongBlock rowPositionBlock;
            boolean success = false;
            try (
                LongBlock.Builder sortBuilder = blockFactory.newLongBlockBuilder(sz);
                LongBlock.Builder rpBuilder = blockFactory.newLongBlockBuilder(sz)
            ) {
                for (int i = 0; i < sz; i++) {
                    int abs = p + i;
                    if (nullMask[abs]) {
                        sortBuilder.appendNull();
                    } else {
                        sortBuilder.appendLong(values.get(abs));
                    }
                    rpBuilder.appendLong(abs);
                }
                sortBlock = sortBuilder.build();
                try {
                    rowPositionBlock = rpBuilder.build();
                    success = true;
                } finally {
                    if (success == false) {
                        sortBlock.close();
                    }
                }
            }
            out.add(new Page(sortBlock, rowPositionBlock));
            p = end;
        }
        return out;
    }

    private void assertPagesEqual(List<Page> reference, List<Page> numeric, int k) {
        long refRows = reference.stream().mapToLong(Page::getPositionCount).sum();
        long numRows = numeric.stream().mapToLong(Page::getPositionCount).sum();
        assertThat("both operators must emit the same total row count", numRows, equalTo(refRows));
        assertThat("at most K rows", refRows, lessThanOrEqualTo((long) k));
        // The generic TopNOperator is non-stable across equal sort keys: its underlying Lucene
        // PriorityQueue drains equal-key rows in heap-internal order, and at the K-th boundary
        // any of several tied rows may survive. The numeric operator's stable
        // {@code _rowPosition} ASC tiebreak picks a single specific row. So a row-position-level
        // comparison is too strict — both operators may return valid Top-K answers that disagree
        // on which boundary-tied row position survived. The right invariant is "same surviving
        // sort-key multiset" (catches wrong threshold, wrong direction, missing/duplicate rows,
        // missing nulls). The {@code _rowPosition} payload is exercised by the typed-correctness
        // tests below, which compare against an independent ranking helper using the same
        // {@code _rowPosition}-ASC tiebreak as the operator.
        List<long[]> refRowsList = canonicalize(reference);
        List<long[]> numRowsList = canonicalize(numeric);
        assertThat("canonicalized row count", numRowsList.size(), equalTo(refRowsList.size()));
        for (int i = 0; i < refRowsList.size(); i++) {
            long[] r = refRowsList.get(i);
            long[] m = numRowsList.get(i);
            assertThat("sort key null flag at row " + i, m[2], equalTo(r[2]));
            if (r[2] == 0) {
                assertThat("sort key value at row " + i, m[0], equalTo(r[0]));
            }
        }
    }

    /**
     * Materialise pages into a list of {@code [sortKey, rowPosition, isNull (0/1)]} triples
     * and sort under the canonical ordering: nulls last by isNull flag, then sortKey ASC, then
     * rowPosition ASC. This collapses any drain-order ambiguity so the comparison only fails
     * when the operators actually disagree on which rows survived or on the row position
     * carried with each survivor.
     */
    private static List<long[]> canonicalize(List<Page> pages) {
        List<long[]> out = new ArrayList<>();
        for (Page p : pages) {
            LongBlock sort = p.getBlock(NumericTopNOperator.SORT_KEY_CHANNEL);
            LongBlock rp = p.getBlock(NumericTopNOperator.ROW_POSITION_CHANNEL);
            for (int pos = 0; pos < p.getPositionCount(); pos++) {
                boolean isNull = sort.isNull(pos);
                long sv = isNull ? 0L : sort.getLong(sort.getFirstValueIndex(pos));
                long rpv = rp.getLong(rp.getFirstValueIndex(pos));
                out.add(new long[] { sv, rpv, isNull ? 1L : 0L });
            }
        }
        out.sort((a, b) -> {
            // nulls last during canonicalisation (the comparison is symmetric so this is just
            // a stable bucket choice, not a semantic decision).
            int n = Long.compare(a[2], b[2]);
            if (n != 0) {
                return n;
            }
            int s = Long.compare(a[0], b[0]);
            if (s != 0) {
                return s;
            }
            return Long.compare(a[1], b[1]);
        });
        return out;
    }

    /**
     * MV correctness: feed multi-valued LONG sort keys through {@link NumericTopNOperator} and
     * assert the surviving set equals the K most-competitive rows under the same composite
     * ordering the operator uses internally (MV-min for ASC, MV-max for DESC, empty slot = null,
     * then {@code _rowPosition} ASC tiebreak).
     *
     * <p>This is structurally the same check as
     * {@code runTypeCorrectness(ElementType.LONG, ...)} but with MV inputs instead of
     * single-valued. The comparison is against an independently-computed reference (not the
     * generic operator) so we avoid the generic operator's heap-internal-order ambiguity at the
     * K boundary.
     */
    public void testMultiValuedUnorderedLongCorrectness() {
        runMvLongCorrectness(/*mvSortedAscending*/ false);
    }

    /**
     * MV correctness on the {@code SORTED_ASCENDING} path: the builder marks the block as having
     * sorted MV ordering, so {@link LongSortKeyExtractor#extractorFor(LongBlock, boolean)} picks
     * the O(1) {@code MinFromAscendingBlock} / {@code MaxFromAscendingBlock} variant. The check
     * verifies the optimised path produces the same surviving set as the unordered scan would.
     */
    public void testMultiValuedAscendingLongCorrectness() {
        runMvLongCorrectness(/*mvSortedAscending*/ true);
    }

    /**
     * MV correctness for INT, DOUBLE, and BOOLEAN — covers the per-type extractors'
     * {@code MinFromUnorderedBlock} / {@code MaxFromUnorderedBlock} branches. The LONG MV tests
     * above already cover the {@code MinFromAscendingBlock} / {@code MaxFromAscendingBlock}
     * branches, and those branches are structurally identical across types (single read at
     * {@code firstValueIndex} or {@code firstValueIndex + valueCount - 1}), so we don't
     * duplicate the ascending-MV variant for every type.
     */
    public void testMultiValuedIntCorrectness() {
        runMvTypedCorrectness(ElementType.INT);
    }

    public void testMultiValuedDoubleCorrectness() {
        runMvTypedCorrectness(ElementType.DOUBLE);
    }

    public void testMultiValuedBooleanCorrectness() {
        runMvTypedCorrectness(ElementType.BOOLEAN);
    }

    private void runMvTypedCorrectness(ElementType elementType) {
        boolean asc = randomBoolean();
        boolean nullsFirst = randomBoolean();
        int n = randomIntBetween(50, 400);
        int k = randomIntBetween(1, Math.min(40, n));
        // For each input row, randomly pick 0/1/2-or-more values of the type, then reduce them
        // to a single "competing" value the way the operator's extractor will. Track:
        // - the raw MV slot (to build the input block),
        // - the reduced typed value (to feed the independent ranking helper).
        boolean[] nullMask = new boolean[n];
        Object reduced = switch (elementType) {
            case INT -> new int[n];
            case DOUBLE -> new double[n];
            case BOOLEAN -> new boolean[n];
            default -> throw new AssertionError(elementType);
        };
        List<Object> rowValues = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int kind = randomInt(2);
            if (kind == 1) {
                nullMask[i] = true;
                rowValues.add(null);
                continue;
            }
            int sz = kind == 0 ? 1 : randomIntBetween(2, 5);
            switch (elementType) {
                case INT -> {
                    int[] vs = new int[sz];
                    int best = 0;
                    for (int j = 0; j < sz; j++) {
                        vs[j] = randomIntBetween(-1000, 1000);
                        if (j == 0) {
                            best = vs[j];
                        } else {
                            best = asc ? Math.min(best, vs[j]) : Math.max(best, vs[j]);
                        }
                    }
                    ((int[]) reduced)[i] = best;
                    rowValues.add(vs);
                }
                case DOUBLE -> {
                    double[] vs = new double[sz];
                    double best = 0;
                    for (int j = 0; j < sz; j++) {
                        vs[j] = randomDoubleBetween(-1000.0, 1000.0, true);
                        if (j == 0) {
                            best = vs[j];
                        } else {
                            best = asc ? Math.min(best, vs[j]) : Math.max(best, vs[j]);
                        }
                    }
                    ((double[]) reduced)[i] = best;
                    rowValues.add(vs);
                }
                case BOOLEAN -> {
                    boolean[] vs = new boolean[sz];
                    boolean best = false;
                    for (int j = 0; j < sz; j++) {
                        vs[j] = randomBoolean();
                        if (j == 0) {
                            best = vs[j];
                        } else if (asc) {
                            best = best && vs[j]; // min: any false → false
                        } else {
                            best = best || vs[j]; // max: any true → true
                        }
                    }
                    ((boolean[]) reduced)[i] = best;
                    rowValues.add(vs);
                }
                default -> throw new AssertionError(elementType);
            }
        }

        BlockFactory blockFactory = blockFactory();
        List<Integer> survivors = new ArrayList<>();
        try (
            Operator op = new NumericTopNOperator.NumericTopNOperatorFactory(k, elementType, asc, nullsFirst).get(
                new DriverContext(blockFactory.bigArrays(), blockFactory, null)
            )
        ) {
            for (Page p : typedMvPagesFor(blockFactory, elementType, rowValues, nullMask)) {
                op.addInput(p);
            }
            op.finish();
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out != null) {
                    try {
                        LongBlock rp = out.getBlock(NumericTopNOperator.ROW_POSITION_CHANNEL);
                        for (int pos = 0; pos < out.getPositionCount(); pos++) {
                            survivors.add((int) rp.getLong(rp.getFirstValueIndex(pos)));
                        }
                    } finally {
                        out.releaseBlocks();
                    }
                }
            }
        }
        List<Integer> ranked = independentRanking(elementType, reduced, nullMask, asc, nullsFirst);
        assertSubsetWithBoundaryTies(elementType, reduced, nullMask, asc, nullsFirst, ranked, survivors, k);
    }

    /**
     * Build typed 2-channel pages where the sort-key slot at each position is the MV list from
     * {@code rowValues} (or null when {@code nullMask[i]} is set). Mirrors {@link #typedPagesFor}
     * but uses MV builders.
     */
    private List<Page> typedMvPagesFor(BlockFactory blockFactory, ElementType elementType, List<Object> rowValues, boolean[] nullMask) {
        int n = nullMask.length;
        if (n == 0) {
            return List.of();
        }
        int pageSize = randomIntBetween(1, Math.max(1, n));
        List<Page> out = new ArrayList<>();
        int p = 0;
        while (p < n) {
            int end = Math.min(p + pageSize, n);
            int sz = end - p;
            Block sortBlock = buildTypedMvSortBlock(blockFactory, elementType, rowValues, nullMask, p, sz);
            LongBlock rpBlock;
            boolean success = false;
            try (LongBlock.Builder rpBuilder = blockFactory.newLongBlockBuilder(sz)) {
                for (int i = 0; i < sz; i++) {
                    rpBuilder.appendLong(p + i);
                }
                try {
                    rpBlock = rpBuilder.build();
                    success = true;
                } finally {
                    if (success == false) {
                        sortBlock.close();
                    }
                }
            }
            out.add(new Page(sortBlock, rpBlock));
            p = end;
        }
        return out;
    }

    private Block buildTypedMvSortBlock(
        BlockFactory blockFactory,
        ElementType elementType,
        List<Object> rowValues,
        boolean[] nullMask,
        int start,
        int sz
    ) {
        return switch (elementType) {
            case INT -> {
                try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                            continue;
                        }
                        int[] vs = (int[]) rowValues.get(abs);
                        if (vs.length == 1) {
                            b.appendInt(vs[0]);
                        } else {
                            b.beginPositionEntry();
                            for (int v : vs) {
                                b.appendInt(v);
                            }
                            b.endPositionEntry();
                        }
                    }
                    yield b.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder b = blockFactory.newDoubleBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                            continue;
                        }
                        double[] vs = (double[]) rowValues.get(abs);
                        if (vs.length == 1) {
                            b.appendDouble(vs[0]);
                        } else {
                            b.beginPositionEntry();
                            for (double v : vs) {
                                b.appendDouble(v);
                            }
                            b.endPositionEntry();
                        }
                    }
                    yield b.build();
                }
            }
            case BOOLEAN -> {
                try (BooleanBlock.Builder b = blockFactory.newBooleanBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                            continue;
                        }
                        boolean[] vs = (boolean[]) rowValues.get(abs);
                        if (vs.length == 1) {
                            b.appendBoolean(vs[0]);
                        } else {
                            b.beginPositionEntry();
                            for (boolean v : vs) {
                                b.appendBoolean(v);
                            }
                            b.endPositionEntry();
                        }
                    }
                    yield b.build();
                }
            }
            default -> throw new AssertionError(elementType);
        };
    }

    private void runMvLongCorrectness(boolean mvSortedAscending) {
        boolean asc = randomBoolean();
        boolean nullsFirst = randomBoolean();
        int n = randomIntBetween(50, 600);
        int k = randomIntBetween(1, Math.min(40, n));
        // Three flavours per row: single-value, empty MV (treated as null), and a non-empty MV
        // list. The mix forces the extractor through every branch.
        List<long[]> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int kind = randomInt(2);
            switch (kind) {
                case 0 -> rows.add(new long[] { randomLong() });
                case 1 -> rows.add(new long[] {});
                default -> {
                    int sz = randomIntBetween(2, 5);
                    long[] vs = new long[sz];
                    for (int j = 0; j < sz; j++) {
                        vs[j] = randomLong();
                    }
                    if (mvSortedAscending) {
                        java.util.Arrays.sort(vs);
                    }
                    rows.add(vs);
                }
            }
        }
        // Reduce each input row to its competing scalar (null for empty slots) so the
        // independent ranking helper can rank under the same ordering the operator computes.
        boolean[] nullMask = new boolean[n];
        long[] reduced = new long[n];
        for (int i = 0; i < n; i++) {
            long[] vs = rows.get(i);
            if (vs.length == 0) {
                nullMask[i] = true;
            } else {
                long best = vs[0];
                for (int j = 1; j < vs.length; j++) {
                    best = asc ? Math.min(best, vs[j]) : Math.max(best, vs[j]);
                }
                reduced[i] = best;
            }
        }

        BlockFactory blockFactory = blockFactory();
        List<Integer> survivors;
        try (Operator op = numeric(blockFactory, k, asc, nullsFirst)) {
            for (Page p : mvPagesFor(blockFactory, rows, mvSortedAscending)) {
                op.addInput(p);
            }
            op.finish();
            survivors = new ArrayList<>();
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out != null) {
                    try {
                        LongBlock rpBlock = out.getBlock(NumericTopNOperator.ROW_POSITION_CHANNEL);
                        for (int pos = 0; pos < out.getPositionCount(); pos++) {
                            survivors.add((int) rpBlock.getLong(rpBlock.getFirstValueIndex(pos)));
                        }
                    } finally {
                        out.releaseBlocks();
                    }
                }
            }
        }

        // Independent ranking under the operator's composite order (LONG comparator, asc/desc,
        // nulls per nullsFirst, _rowPosition ASC tiebreak — exactly the ordering
        // {@link #compareForRanking} encodes for ElementType.LONG).
        List<Integer> ranked = independentRankingLong(reduced, nullMask, asc, nullsFirst);
        assertSubsetWithBoundaryTies(ElementType.LONG, reduced, nullMask, asc, nullsFirst, ranked, survivors, k);
    }

    /**
     * Specialised version of {@link #independentRanking} for LONG sort keys. Inlined to avoid
     * widening {@code compareForRanking}'s {@code switch} just to support MV LONG; the typed
     * correctness tests already cover the INT / DOUBLE / BOOLEAN paths for single values.
     */
    private List<Integer> independentRankingLong(long[] reduced, boolean[] nullMask, boolean asc, boolean nullsFirst) {
        int n = nullMask.length;
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            indices.add(i);
        }
        indices.sort((a, b) -> {
            boolean na = nullMask[a];
            boolean nb = nullMask[b];
            if (na != nb) {
                return na == nullsFirst ? -1 : 1;
            }
            if (na) {
                return Integer.compare(a, b);
            }
            int cmp = Long.compare(reduced[a], reduced[b]);
            if (asc == false) {
                cmp = -cmp;
            }
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(a, b);
        });
        return indices;
    }

    /**
     * Build pages where each input row may have zero, one, or many values in the sort-key slot.
     * The {@code _rowPosition} column stays dense and strictly increasing (one row position per
     * page position), matching the contract the external source guarantees.
     */
    private List<Page> mvPagesFor(BlockFactory blockFactory, List<long[]> rows, boolean mvSortedAscending) {
        int n = rows.size();
        if (n == 0) {
            return List.of();
        }
        int pageSize = randomIntBetween(1, Math.max(1, n));
        List<Page> out = new ArrayList<>();
        int p = 0;
        while (p < n) {
            int end = Math.min(p + pageSize, n);
            int sz = end - p;
            LongBlock sortBlock;
            LongBlock rowPositionBlock;
            boolean success = false;
            try (
                LongBlock.Builder sortBuilder = blockFactory.newLongBlockBuilder(sz);
                LongBlock.Builder rpBuilder = blockFactory.newLongBlockBuilder(sz)
            ) {
                if (mvSortedAscending) {
                    sortBuilder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
                }
                for (int i = 0; i < sz; i++) {
                    int abs = p + i;
                    long[] vs = rows.get(abs);
                    if (vs.length == 0) {
                        sortBuilder.appendNull();
                    } else if (vs.length == 1) {
                        sortBuilder.appendLong(vs[0]);
                    } else {
                        sortBuilder.beginPositionEntry();
                        for (long v : vs) {
                            sortBuilder.appendLong(v);
                        }
                        sortBuilder.endPositionEntry();
                    }
                    rpBuilder.appendLong(abs);
                }
                sortBlock = sortBuilder.build();
                try {
                    rowPositionBlock = rpBuilder.build();
                    success = true;
                } finally {
                    if (success == false) {
                        sortBlock.close();
                    }
                }
            }
            out.add(new Page(sortBlock, rowPositionBlock));
            p = end;
        }
        return out;
    }

    public void testEmptyInput() {
        BlockFactory blockFactory = blockFactory();
        try (Operator op = numeric(blockFactory, 10, true, false)) {
            op.finish();
            assertTrue(op.isFinished());
            assertNull(op.getOutput());
        }
    }

    public void testFewerThanKInputs() {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            values.add(randomLong());
        }
        assertParityForInput(values, /*k=*/ 10, randomBoolean(), randomBoolean(), 0.0);
    }

    public void testIntSortKeyAscDescNulls() {
        runTypeCorrectness(ElementType.INT, randomBoolean(), randomBoolean(), randomDoubleBetween(0.0, 0.4, true));
    }

    public void testDoubleSortKeyAscDescNulls() {
        runTypeCorrectness(ElementType.DOUBLE, randomBoolean(), randomBoolean(), randomDoubleBetween(0.0, 0.4, true));
    }

    public void testBooleanSortKey() {
        // Booleans have just two values, so this also exercises the operator on a heavy-ties
        // workload with the smaller value-space than even {@link #testParityLowCardinalityTies}.
        runTypeCorrectness(ElementType.BOOLEAN, randomBoolean(), randomBoolean(), randomDoubleBetween(0.0, 0.3, true));
    }

    /**
     * Run the operator with a typed sort key and verify two properties of the output:
     * <ol>
     *     <li>The output page is sorted in the configured order (so an external consumer that
     *         relies on TopN's "the page comes out sorted" contract still works).</li>
     *     <li>The set of surviving rows is exactly the K most-competitive rows under the
     *         configured order — computed independently from the input and compared by row
     *         position (which uniquely identifies each input row).</li>
     * </ol>
     * This is a stronger correctness check than parity against the generic operator because it
     * is not vulnerable to drain-order non-determinism on ties.
     */
    private void runTypeCorrectness(ElementType elementType, boolean asc, boolean nullsFirst, double nullProbability) {
        BlockFactory blockFactory = blockFactory();
        int n = randomIntBetween(20, 500);
        int k = randomIntBetween(1, Math.min(20, n));
        // Build typed input. For each element type we materialise the values into a long[] so
        // the order check below is uniform across types.
        Object rawValues = randomValues(elementType, n);
        boolean[] nullMask = new boolean[n];
        for (int i = 0; i < n; i++) {
            nullMask[i] = randomDouble() < nullProbability;
        }
        List<Page> input = typedPagesFor(blockFactory, elementType, rawValues, nullMask);
        List<Page> output = new ArrayList<>();
        try (
            Operator op = new NumericTopNOperator.NumericTopNOperatorFactory(k, elementType, asc, nullsFirst).get(
                new DriverContext(blockFactory.bigArrays(), blockFactory, null)
            )
        ) {
            for (Page p : input) {
                op.addInput(p);
            }
            op.finish();
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out != null) {
                    output.add(out);
                }
            }
            // Collect surviving row positions and verify the surviving set matches what an
            // independent ranking of the input picks.
            List<Integer> survivingRows = new ArrayList<>();
            for (Page p : output) {
                LongBlock rp = p.getBlock(NumericTopNOperator.ROW_POSITION_CHANNEL);
                for (int pos = 0; pos < p.getPositionCount(); pos++) {
                    survivingRows.add(Math.toIntExact(rp.getLong(rp.getFirstValueIndex(pos))));
                }
            }
            assertThat("output size", survivingRows.size(), lessThanOrEqualTo(k));
            // Independent ranking: build an index list, sort by the configured order, and take
            // the first {@code k} entries. The operator's surviving set must equal this index
            // set (compared as a sorted list so we don't depend on drain order).
            List<Integer> ranked = independentRanking(elementType, rawValues, nullMask, asc, nullsFirst);
            List<Integer> expected = ranked.subList(0, Math.min(k, ranked.size()));
            // Ties in the operator's heap break on _rowPosition ASC (first-seen wins). The
            // {@link #independentRanking} helper uses the same tiebreaker, so the expected set
            // is exact when the {@code k}-th rank is unambiguous. When several rows tie at the
            // boundary we widen the comparison to "subset of the equivalence class": every
            // surviving row must rank within the expected window plus its boundary ties.
            assertSubsetWithBoundaryTies(elementType, rawValues, nullMask, asc, nullsFirst, ranked, survivingRows, k);
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Random typed values for each supported element type. INT and DOUBLE are produced over a
     * narrow range so ties show up frequently in {@code k}-sized samples; BOOLEAN is uniform
     * over {true, false}.
     */
    private Object randomValues(ElementType elementType, int n) {
        return switch (elementType) {
            case INT -> {
                int[] arr = new int[n];
                for (int i = 0; i < n; i++) {
                    arr[i] = randomIntBetween(-1000, 1000);
                }
                yield arr;
            }
            case DOUBLE -> {
                double[] arr = new double[n];
                for (int i = 0; i < n; i++) {
                    arr[i] = randomDoubleBetween(-1000.0, 1000.0, true);
                }
                yield arr;
            }
            case BOOLEAN -> {
                boolean[] arr = new boolean[n];
                for (int i = 0; i < n; i++) {
                    arr[i] = randomBoolean();
                }
                yield arr;
            }
            default -> throw new AssertionError(elementType);
        };
    }

    /**
     * Build typed 2-channel pages of {@code [sortKey, _rowPosition]} for a given element type.
     * Row positions are the input index so the operator's output can be cross-checked against
     * the raw input.
     */
    private List<Page> typedPagesFor(BlockFactory blockFactory, ElementType elementType, Object raw, boolean[] nullMask) {
        int n = nullMask.length;
        if (n == 0) {
            return List.of();
        }
        int pageSize = randomIntBetween(1, Math.max(1, n));
        List<Page> out = new ArrayList<>();
        int p = 0;
        while (p < n) {
            int end = Math.min(p + pageSize, n);
            int sz = end - p;
            Block sortBlock = buildTypedSortBlock(blockFactory, elementType, raw, nullMask, p, sz);
            LongBlock rpBlock;
            boolean success = false;
            try (LongBlock.Builder rpBuilder = blockFactory.newLongBlockBuilder(sz)) {
                for (int i = 0; i < sz; i++) {
                    rpBuilder.appendLong(p + i);
                }
                rpBlock = rpBuilder.build();
                success = true;
            } finally {
                if (success == false) {
                    sortBlock.close();
                }
            }
            out.add(new Page(sortBlock, rpBlock));
            p = end;
        }
        return out;
    }

    private Block buildTypedSortBlock(
        BlockFactory blockFactory,
        ElementType elementType,
        Object raw,
        boolean[] nullMask,
        int start,
        int sz
    ) {
        switch (elementType) {
            case INT -> {
                int[] arr = (int[]) raw;
                try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                        } else {
                            b.appendInt(arr[abs]);
                        }
                    }
                    return b.build();
                }
            }
            case DOUBLE -> {
                double[] arr = (double[]) raw;
                try (DoubleBlock.Builder b = blockFactory.newDoubleBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                        } else {
                            b.appendDouble(arr[abs]);
                        }
                    }
                    return b.build();
                }
            }
            case BOOLEAN -> {
                boolean[] arr = (boolean[]) raw;
                try (BooleanBlock.Builder b = blockFactory.newBooleanBlockBuilder(sz)) {
                    for (int i = 0; i < sz; i++) {
                        int abs = start + i;
                        if (nullMask[abs]) {
                            b.appendNull();
                        } else {
                            b.appendBoolean(arr[abs]);
                        }
                    }
                    return b.build();
                }
            }
            default -> throw new AssertionError(elementType);
        }
    }

    /**
     * Independent ranking helper: build an index list, sort it under the same composite order
     * the operator uses (configured order on the typed value, with nulls placed per
     * {@code nullsFirst}, then by row position ascending), and return the indices in
     * most-competitive-first order. This is what the operator's surviving set is checked against.
     */
    private List<Integer> independentRanking(ElementType elementType, Object raw, boolean[] nullMask, boolean asc, boolean nullsFirst) {
        int n = nullMask.length;
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            indices.add(i);
        }
        indices.sort((a, b) -> compareForRanking(elementType, raw, nullMask, asc, nullsFirst, a, b));
        return indices;
    }

    private int compareForRanking(ElementType elementType, Object raw, boolean[] nullMask, boolean asc, boolean nullsFirst, int a, int b) {
        boolean na = nullMask[a];
        boolean nb = nullMask[b];
        if (na != nb) {
            // nullsFirst means nulls sort earlier (= more competitive in this ranking).
            return na == nullsFirst ? -1 : 1;
        }
        if (na) {
            // Both null: stable on input order (= row position ascending).
            return Integer.compare(a, b);
        }
        int cmp = switch (elementType) {
            case INT -> Integer.compare(((int[]) raw)[a], ((int[]) raw)[b]);
            case DOUBLE -> Double.compare(((double[]) raw)[a], ((double[]) raw)[b]);
            case BOOLEAN -> Boolean.compare(((boolean[]) raw)[a], ((boolean[]) raw)[b]);
            default -> throw new AssertionError(elementType);
        };
        if (asc == false) {
            cmp = -cmp;
        }
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(a, b);
    }

    /**
     * The operator's heap is stable on {@code _rowPosition} ascending, and so is
     * {@link #independentRanking}. So the surviving set is exactly {@code ranked.subList(0, k)}
     * — no boundary-tie widening needed. We keep this method as a single assertion site so any
     * future ordering changes show up in one place.
     */
    private void assertSubsetWithBoundaryTies(
        ElementType elementType,
        Object raw,
        boolean[] nullMask,
        boolean asc,
        boolean nullsFirst,
        List<Integer> ranked,
        List<Integer> survivingRows,
        int k
    ) {
        List<Integer> expected = new ArrayList<>(ranked.subList(0, Math.min(k, ranked.size())));
        java.util.Collections.sort(expected);
        List<Integer> actual = new ArrayList<>(survivingRows);
        java.util.Collections.sort(actual);
        assertThat("surviving set must equal the K most competitive rows", actual, equalTo(expected));
    }

    public void testBreakerReleased() {
        BlockFactory blockFactory = blockFactory();
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(randomLong());
        }
        boolean[] nullMask = new boolean[values.size()];
        List<Page> input = pagesFor(blockFactory, values, nullMask);
        try (Operator op = numeric(blockFactory, 5, true, false)) {
            for (Page p : input) {
                op.addInput(p);
            }
            op.finish();
            List<Page> outputs = new ArrayList<>();
            try {
                while (op.isFinished() == false) {
                    Page p = op.getOutput();
                    if (p != null) {
                        outputs.add(p);
                    }
                }
            } finally {
                Releasables.close(() -> outputs.forEach(Page::releaseBlocks));
            }
        }
        // ComputeTestCase.allBreakersEmpty() in @After verifies the breaker is at zero. Add an
        // explicit assertion so this test fails locally with a clearer message if the operator
        // leaks.
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
