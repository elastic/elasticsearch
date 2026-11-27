/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Emits one row per child document that a container column carries, repeating the parent columns, and appends one block per
 * child field. A parent whose container has no value keeps one row with null child values. A parent whose container holds
 * several values, one for each document the lookup matched, gets the children of all of them. Each input page becomes one
 * output page, so its size grows with the number of children.
 */
public final class NestedExpandFromContainerOperator extends AbstractPageMappingOperator {
    static final String BREAKER_LABEL = "nested_expand";

    public record Factory(int containerChannel, List<DataType> types) implements Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedExpandFromContainerOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedExpandFromContainerOperator[columns=" + types.size() + "]";
        }
    }

    private final DriverContext context;
    private final Factory factory;

    private NestedExpandFromContainerOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
    }

    @Override
    protected Page process(Page page) {
        try {
            return expand(page);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            page.releaseBlocks();
        }
    }

    private Page expand(Page page) throws IOException {
        int columns = factory.types().size();
        Block.Builder[] built = new Block.Builder[columns];
        Block[] blocks = new Block[columns];
        int[] positions = new int[page.getPositionCount()];
        int count = 0;
        long reserved = 0;
        try {
            for (int c = 0; c < columns; c++) {
                built[c] = PlannerUtils.toElementType(factory.types().get(c))
                    .newBlockBuilder(page.getPositionCount(), context.blockFactory());
            }
            BytesRefBlock container = page.getBlock(factory.containerChannel());
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                int rows = 0;
                int values = container.getValueCount(p);
                for (int v = 0; v < values; v++) {
                    BytesRef bytes = container.getBytesRef(container.getFirstValueIndex(p) + v, scratch);
                    try (Page children = NestedContainerCodec.decode(context.blockFactory(), bytes)) {
                        for (int c = 0; c < columns; c++) {
                            built[c].copyFrom(children.getBlock(c), 0, children.getPositionCount());
                        }
                        rows += children.getPositionCount();
                    }
                }
                if (rows == 0) {
                    for (Block.Builder builder : built) {
                        builder.appendNull();
                    }
                    rows = 1;
                }
                if (count + rows > positions.length) {
                    int size = ArrayUtil.oversize(count + rows, Integer.BYTES);
                    long bytes = (long) (size - positions.length) * Integer.BYTES;
                    context.breaker().addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
                    reserved += bytes;
                    positions = ArrayUtil.growExact(positions, size);
                }
                for (int r = 0; r < rows; r++) {
                    positions[count++] = p;
                }
            }
            for (int c = 0; c < columns; c++) {
                blocks[c] = built[c].build();
            }
            Page parents = null;
            try {
                parents = page.filter(true, positions, 0, count);
                Page result = parents.appendBlocks(blocks);
                parents = null;
                blocks = null;
                return result;
            } finally {
                if (parents != null) {
                    parents.releaseBlocks();
                }
            }
        } finally {
            context.breaker().addWithoutBreaking(-reserved);
            Releasables.closeExpectNoException(built);
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public String toString() {
        return factory.describe();
    }
}
