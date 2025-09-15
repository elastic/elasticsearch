/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * {@link SourceOperator} that returns a sequence of pre-built {@link Page}s.
 */
public class CannedSourceOperator extends SourceOperator {
    public static List<Page> collectPages(SourceOperator source) {
        try {
            List<Page> pages = new ArrayList<>();
            while (source.isFinished() == false) {
                Page in = source.getOutput();
                if (in == null) {
                    continue;
                }
                if (in.getPositionCount() == 0) {
                    in.releaseBlocks();
                    continue;
                }
                pages.add(in);
            }
            return pages;
        } finally {
            source.close();
        }
    }

    public static Page mergePages(List<Page> pages) {
        int totalPositions = pages.stream().mapToInt(Page::getPositionCount).sum();
        Page first = pages.get(0);
        Block.Builder[] builders = new Block.Builder[first.getBlockCount()];
        try {
            for (int b = 0; b < builders.length; b++) {
                builders[b] = first.getBlock(b).elementType().newBlockBuilder(totalPositions, TestBlockFactory.getNonBreakingInstance());
            }
            for (Page p : pages) {
                for (int b = 0; b < builders.length; b++) {
                    builders[b].copyFrom(p.getBlock(b), 0, p.getPositionCount());
                }
            }
            Block[] blocks = new Block[builders.length];
            Page result = null;
            try {
                for (int b = 0; b < blocks.length; b++) {
                    blocks[b] = builders[b].build();
                }
                result = new Page(blocks);
            } finally {
                if (result == null) {
                    Releasables.close(blocks);
                }
            }
            return result;
        } finally {
            Iterable<Releasable> releasePages = () -> Iterators.map(pages.iterator(), p -> p::releaseBlocks);
            Releasables.closeExpectNoException(Releasables.wrap(builders), Releasables.wrap(releasePages));
        }
    }

    /**
     * Make a deep copy of some pages. Useful so that when the originals are
     * released the copies are still live.
     */
    public static List<Page> deepCopyOf(BlockFactory blockFactory, List<Page> pages) {
        List<Page> out = new ArrayList<>(pages.size());
        try {
            for (Page p : pages) {
                Block[] blocks = new Block[p.getBlockCount()];
                for (int b = 0; b < blocks.length; b++) {
                    Block orig = p.getBlock(b);
                    try (Block.Builder builder = orig.elementType().newBlockBuilder(p.getPositionCount(), blockFactory)) {
                        builder.copyFrom(orig, 0, p.getPositionCount());
                        blocks[b] = builder.build();
                    }
                }
                out.add(new Page(blocks));
            }
        } finally {
            if (pages.size() != out.size()) {
                // failed to copy all the pages, we're bubbling out an exception. So we have to close the copy.
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(out.iterator(), p -> p::releaseBlocks)));
            }
        }
        return out;
    }

    private final Iterator<Page> page;

    public CannedSourceOperator(Iterator<Page> page) {
        this.page = page;
    }

    @Override
    public void finish() {
        while (page.hasNext()) {
            page.next();
        }
    }

    @Override
    public boolean isFinished() {
        return false == page.hasNext();
    }

    @Override
    public Page getOutput() {
        return page.next();
    }

    @Override
    public void close() {
        // release pages in the case of early termination - failure
        while (page.hasNext()) {
            page.next().releaseBlocks();
        }
    }
}
