/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The block-level join of an expanding read, on blocks built by hand so each case states its parents, its drops and its
 * elements exactly. Every case runs on a limited breaker that must be back at zero once the result is released.
 */
public class ExpandedRowsTests extends ESTestCase {

    private LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
    private BlockFactory blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);

    /** Parents repeat once per element, a parent without elements keeps one null row, all in parent order. */
    public void testRepeatsParentsAndFillsNullRows() {
        Page out = ExpandedRows.expand(
            parents(10, 11, 12, 13),
            null,
            4,
            positions(0, 0, 2, 2, 2),
            new Block[] { longs(100, 101, 120, 121, 122) },
            blockFactory
        );
        assertThat(
            rows(out),
            equalTo(
                List.of(
                    Arrays.asList(10L, 100L),
                    Arrays.asList(10L, 101L),
                    Arrays.asList(11L, null),
                    Arrays.asList(12L, 120L),
                    Arrays.asList(12L, 121L),
                    Arrays.asList(12L, 122L),
                    Arrays.asList(13L, null)
                )
            )
        );
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The elements of a parent the batch dropped go with it, whether it had elements or not, and the survivors keep
     * their own elements beside their own values.
     */
    public void testDropsTheElementsOfDroppedParents() {
        Page out = ExpandedRows.expand(
            parents(10, 13),
            new int[] { 0, 3 },
            4,
            positions(0, 1, 1, 3),
            new Block[] { longs(100, 110, 111, 130) },
            blockFactory
        );
        assertThat(rows(out), equalTo(List.of(Arrays.asList(10L, 100L), Arrays.asList(13L, 130L))));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A column no element of the batch has is null on every row, null rows included. */
    public void testMissingColumnIsNull() {
        Page out = ExpandedRows.expand(parents(10, 11), null, 2, positions(0, 0), new Block[] { null, longs(1, 2) }, blockFactory);
        assertThat(rows(out), equalTo(List.of(Arrays.asList(10L, null, 1L), Arrays.asList(10L, null, 2L), Arrays.asList(11L, null, null))));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A page with no parent column still has a row per element and per null row. */
    public void testNoParentColumns() {
        Page out = ExpandedRows.expand(new Page(3), null, 3, positions(1), new Block[] { longs(7) }, blockFactory);
        assertThat(out.getPositionCount(), equalTo(3));
        assertThat(rows(out), equalTo(List.of(Arrays.asList((Object) null), List.of(7L), Arrays.asList((Object) null))));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** Parents that do not match what the batch says it kept are refused, and everything handed over is released. */
    public void testMismatchedCountsRefusedAndReleased() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ExpandedRows.expand(parents(1, 2), new int[] { 0 }, 3, positions(0), new Block[] { longs(5) }, blockFactory)
        );
        assertThat(e.getMessage(), containsString("[2] parent rows but the batch kept [1]"));
        assertThat(breaker.getUsed(), equalTo(0L));
        e = expectThrows(
            IllegalStateException.class,
            () -> ExpandedRows.expand(parents(1), null, 1, positions(0, 0), new Block[] { longs(5) }, blockFactory)
        );
        assertThat(e.getMessage(), containsString("an element column has [1] positions for [2] elements"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Whatever limit the join's own allocations trip the breaker at, nothing stays charged once the failure has
     * propagated: neither the inputs it took over, charged to their own breaker, nor what it built, charged to the
     * limited one. The inputs drop one parent so the join filters every column rather than reusing it.
     */
    public void testReleasesEverythingWhenTheBreakerTrips() {
        int tripped = 0;
        int finished = 0;
        LimitedBreaker inputs = breaker;
        for (long limit = 256; limit <= 256 * 1024; limit *= 2) {
            LimitedBreaker joinBreaker = new LimitedBreaker("join", ByteSizeValue.ofBytes(limit));
            BlockFactory joinFactory = new BlockFactory(joinBreaker, BigArrays.NON_RECYCLING_INSTANCE);
            long[] parentValues = new long[199];
            int[] survivors = new int[199];
            for (int p = 0; p < survivors.length; p++) {
                survivors[p] = p < 100 ? p : p + 1;
            }
            int[] elementParents = new int[600];
            long[] elementValues = new long[600];
            for (int i = 0; i < elementParents.length; i++) {
                elementParents[i] = i / 3;
                elementValues[i] = i;
            }
            try {
                Page out = ExpandedRows.expand(
                    parents(parentValues),
                    survivors,
                    200,
                    positions(elementParents),
                    new Block[] { longs(elementValues), null },
                    joinFactory
                );
                assertThat(out.getPositionCount(), equalTo(597));
                out.releaseBlocks();
                finished++;
            } catch (CircuitBreakingException e) {
                tripped++;
            }
            assertThat("join breaker at limit " + limit, joinBreaker.getUsed(), equalTo(0L));
            assertThat("input breaker at limit " + limit, inputs.getUsed(), equalTo(0L));
        }
        assertThat(tripped, greaterThan(0));
        assertThat(finished, greaterThan(0));
    }

    private Page parents(long... values) {
        return new Page(longs(values));
    }

    private LongBlock longs(long... values) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(values.length)) {
            for (long value : values) {
                builder.appendLong(value);
            }
            return builder.build();
        }
    }

    private IntVector positions(int... values) {
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(values.length)) {
            for (int value : values) {
                builder.appendInt(value);
            }
            return builder.build();
        }
    }

    /** The rows of {@code page}, which this releases. */
    private static List<List<Object>> rows(Page page) {
        try {
            List<List<Object>> rows = new ArrayList<>();
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>();
                for (int b = 0; b < page.getBlockCount(); b++) {
                    row.add(BlockUtils.toJavaObject(page.getBlock(b), p));
                }
                rows.add(row);
            }
            return rows;
        } finally {
            page.releaseBlocks();
        }
    }
}
