/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The elements of one collection for one parent page, as pages of at most {@link #MAX_ELEMENTS_PER_PAGE} elements.
 * Block 0 of each page is an {@link IntVector} of parent positions, non-decreasing, into the parent page; then one block
 * per requested leaf, in request order. A parent's elements may continue on the next page, and a parent with none has
 * no position at all.
 * <p>
 * The pages are read once, forward: {@link #next} hands each page to the caller, who then owns it, and {@link #close}
 * releases the pages not taken. One consumer reads a collection's elements, so nothing keeps a page for a second one.
 */
public final class ChildWindows implements Releasable {

    /**
     * The most elements a page holds. Equal to the page size the index side's nested children are emitted in, so both
     * sources hand consumers pages of the same bound.
     */
    public static final int MAX_ELEMENTS_PER_PAGE = 1024;

    private int parentCount;
    private List<Page> pages;
    private boolean closed;
    /** Index of the next page {@link #next} hands out. */
    private int taken;

    /**
     * Takes ownership of {@code pages}, which address a parent page of {@code parentCount} positions.
     */
    public ChildWindows(int parentCount, List<Page> pages) {
        this.parentCount = parentCount;
        this.pages = new ArrayList<>(pages);
    }

    /** Windows with no elements, for a parent page whose file has no such collection. */
    public static ChildWindows empty(int parentCount) {
        return new ChildWindows(parentCount, List.of());
    }

    /** The positions of the parent page these windows address. */
    public int parentCount() {
        ensureOpen();
        return parentCount;
    }

    /** Whether {@link #next} has another page. */
    public boolean hasNext() {
        ensureOpen();
        return taken < pages.size();
    }

    /** The next page of elements; the caller owns it and releases it. */
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page page = pages.get(taken);
        pages.set(taken++, null);
        return page;
    }

    /** The elements on the pages not yet taken. */
    public long elementCount() {
        ensureOpen();
        long count = 0;
        for (int p = taken; p < pages.size(); p++) {
            count += pages.get(p).getPositionCount();
        }
        return count;
    }

    /**
     * Narrows the windows to the parents a row drop kept: elements of a dropped parent go, and every parent position is
     * renumbered to its index in {@code survivingParentPositions}, which is ascending and addresses the parent page as
     * it was before the drop. Any layer that drops parent rows calls this with the rows it kept, so the positions always
     * address the page the consumer holds.
     */
    public void retainParents(int[] survivingParentPositions) {
        ensureOpen();
        if (taken > 0) {
            throw new IllegalStateException("parents can only be narrowed before the first page of elements is taken");
        }
        int[] renumbered = new int[parentCount];
        Arrays.fill(renumbered, -1);
        int previous = -1;
        for (int i = 0; i < survivingParentPositions.length; i++) {
            int position = survivingParentPositions[i];
            if (position <= previous || position >= parentCount) {
                throw new IllegalArgumentException(
                    "surviving parent positions must ascend within [0, "
                        + parentCount
                        + "), got "
                        + Arrays.toString(survivingParentPositions)
                );
            }
            renumbered[position] = i;
            previous = position;
        }
        List<Page> retained = new ArrayList<>(pages.size());
        try {
            for (int p = 0; p < pages.size(); p++) {
                Page page = pages.get(p);
                Page narrowed = retain(page, renumbered);
                pages.set(p, null);
                if (narrowed != null) {
                    retained.add(narrowed);
                }
            }
        } catch (RuntimeException e) {
            for (Page page : retained) {
                page.releaseBlocks();
            }
            for (Page page : pages) {
                if (page != null) {
                    page.releaseBlocks();
                }
            }
            pages = new ArrayList<>();
            closed = true;
            throw e;
        }
        pages = retained;
        parentCount = survivingParentPositions.length;
    }

    /**
     * {@code page} restricted to the elements whose parent survives, with renumbered parents; {@code null} when none do.
     * Releases {@code page} in every case.
     */
    private static Page retain(Page page, int[] renumbered) {
        try {
            IntBlock parents = page.getBlock(0);
            int count = page.getPositionCount();
            int[] keep = new int[count];
            int kept = 0;
            boolean renumberedInPlace = true;
            for (int e = 0; e < count; e++) {
                int parent = parents.getInt(e);
                int to = renumbered[parent];
                if (to >= 0) {
                    keep[kept++] = e;
                    renumberedInPlace &= to == parent;
                }
            }
            if (kept == 0) {
                return null;
            }
            if (kept == count && renumberedInPlace) {
                Block[] same = new Block[page.getBlockCount()];
                for (int b = 0; b < same.length; b++) {
                    same[b] = page.getBlock(b);
                    same[b].incRef();
                }
                return new Page(count, same);
            }
            int[] positions = Arrays.copyOf(keep, kept);
            Block[] blocks = new Block[page.getBlockCount()];
            try {
                try (IntVector.FixedBuilder builder = parents.blockFactory().newIntVectorFixedBuilder(kept)) {
                    for (int i = 0; i < kept; i++) {
                        builder.appendInt(renumbered[parents.getInt(positions[i])]);
                    }
                    blocks[0] = builder.build().asBlock();
                }
                for (int b = 1; b < blocks.length; b++) {
                    blocks[b] = page.getBlock(b).filter(false, positions);
                }
                return new Page(kept, blocks);
            } catch (RuntimeException e) {
                Releasables.closeExpectNoException(blocks);
                throw e;
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("child windows were already released");
        }
    }

    /** Whether {@link #close} ran, by the consumer or by the iterator when it moved to the next parent page. */
    public boolean isReleased() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Page page : pages) {
            if (page != null) {
                page.releaseBlocks();
            }
        }
        pages = List.of();
    }
}
