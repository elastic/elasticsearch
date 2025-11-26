/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.function.LongFunction;

public class SessionUtils {

    private SessionUtils() {}

    public static Block[] fromPages(List<Attribute> schema, List<Page> pages, BlockFactory blockFactory) {
        int positionCount = pages.stream().mapToInt(Page::getPositionCount).sum();
        Block.Builder[] builders = new Block.Builder[schema.size()];
        Block[] blocks;
        try {
            for (int b = 0; b < builders.length; b++) {
                builders[b] = PlannerUtils.toElementType(schema.get(b).dataType()).newBlockBuilder(positionCount, blockFactory);
            }
            for (Page p : pages) {
                for (int b = 0; b < builders.length; b++) {
                    builders[b].copyFrom(p.getBlock(b), 0, p.getPositionCount());
                }
            }
            blocks = Block.Builder.buildAll(builders);
        } finally {
            Releasables.closeExpectNoException(builders);
        }
        return blocks;
    }

    public static long checkPagesBelowSize(List<Page> pages, ByteSizeValue maxSize, LongFunction<String> exceptionMessage) {
        long currentSize = pages.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
        if (currentSize > maxSize.getBytes()) {
            throw new IllegalArgumentException(exceptionMessage.apply(currentSize));
        }
        return currentSize;
    }
}
