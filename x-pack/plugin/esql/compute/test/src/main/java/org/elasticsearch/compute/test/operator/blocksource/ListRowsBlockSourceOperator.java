/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.operator.blocksource;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockBuilder;
import org.elasticsearch.core.Releasables;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * A source operator whose output is rows specified as a list {@link List} values.
 */
public class ListRowsBlockSourceOperator extends AbstractBlockSourceOperator {
    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<ElementType> types;
    private final List<? extends List<?>> rows;

    public ListRowsBlockSourceOperator(BlockFactory blockFactory, List<ElementType> types, List<? extends List<?>> rows) {
        super(blockFactory, DEFAULT_MAX_PAGE_POSITIONS);
        this.types = types;
        this.rows = rows;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        TestBlockBuilder[] blocks = new TestBlockBuilder[types.size()];
        try {
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = TestBlockBuilder.builderOf(blockFactory, types.get(b));
            }
            for (int i = 0; i < length; i++) {
                List<?> row = rows.get(positionOffset + i);
                assertThat(row, hasSize(types.size()));
                for (int b = 0; b < blocks.length; b++) {
                    Object v = row.get(b);
                    if (v == null) {
                        blocks[b].appendNull();
                        continue;
                    }
                    if (v instanceof List<?> l) {
                        blocks[b].beginPositionEntry();
                        for (Object listValue : l) {
                            blocks[b].appendObject(listValue);
                        }
                        blocks[b].endPositionEntry();
                        continue;
                    }
                    blocks[b].appendObject(v);
                }
            }
            Page page = new Page(Block.Builder.buildAll(blocks));
            currentPosition += page.getPositionCount();
            return page;
        } finally {
            Releasables.close(blocks);
        }
    }

    @Override
    protected int remaining() {
        return rows.size() - currentPosition;
    }

    public List<ElementType> elementTypes() {
        return types;
    }
}
