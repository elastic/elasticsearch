/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Emits one row per object of a mapped nested field, read from each parent's stored source. Parent blocks are
 * repeated once per object; one block per child field is appended. A parent without objects keeps one row with
 * null child values. Each input page becomes one output page, so its size grows with the number of objects.
 */
public final class NestedExpandOperator extends AbstractPageMappingOperator {
    public record Factory(
        int sourceChannel,
        int indexChannel,
        String path,
        Set<String> indices,
        List<DataType> types,
        List<String> paths,
        List<Set<String>> mappedIndices
    ) implements Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedExpandOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedExpandOperator[path=" + path + "]";
        }
    }

    private final DriverContext context;
    private final Factory factory;

    private NestedExpandOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
    }

    @Override
    protected Page process(Page page) {
        int columns = factory.types().size();
        BlockUtils.BuilderWrapper[] children = new BlockUtils.BuilderWrapper[columns];
        Block[] built = new Block[columns];
        Page parents = null;
        try {
            for (int c = 0; c < columns; c++) {
                children[c] = BlockUtils.wrapperFor(
                    context.blockFactory(),
                    PlannerUtils.toElementType(factory.types().get(c)),
                    page.getPositionCount()
                );
            }
            Block source = page.getBlock(factory.sourceChannel());
            Block index = page.getBlock(factory.indexChannel());
            int[] positions = new int[page.getPositionCount()];
            int rows = 0;
            for (int row = 0; row < page.getPositionCount(); row++) {
                context.checkForEarlyTermination();
                String indexName = NestedSource.singleMetadata(index, row, "_index").utf8ToString();
                if (indexName.isEmpty()) {
                    throw new IllegalArgumentException("NESTED_EXPAND requires non-empty _index metadata");
                }
                List<Map<?, ?>> objects = List.of();
                long estimate = 0;
                ParsedContentReservation parsedSource = null;
                try {
                    if (factory.indices().contains(indexName)) {
                        if (source.isNull(row)) {
                            throw new IllegalArgumentException("NESTED_EXPAND source-backed PoC requires stored, unfiltered _source");
                        }
                        BytesRef bytes = NestedSource.singleMetadata(source, row, "_source");
                        estimate = 256L + 8L * bytes.length;
                        context.breaker().addEstimateBytesAndMaybeBreak(estimate, "nested_expand_frame");
                        // Parser charges outlive parser close: decoded objects stay in use until this parent's rows are built.
                        parsedSource = new ParsedContentReservation(context.breaker(), true);
                        objects = NestedSource.objects(factory.path(), bytes, parsedSource, context);
                    }
                    int count = Math.max(1, objects.size());
                    positions = ArrayUtil.grow(positions, rows + count);
                    for (int o = 0; o < count; o++) {
                        if ((o & 1023) == 1023) {
                            context.checkForEarlyTermination();
                        }
                        positions[rows++] = row;
                        Map<?, ?> object = objects.isEmpty() ? null : objects.get(o);
                        for (int c = 0; c < columns; c++) {
                            children[c].accept(
                                object != null && factory.mappedIndices().get(c).contains(indexName)
                                    ? NestedSource.childValue(object, factory.paths().get(c), factory.types().get(c), context)
                                    : null
                            );
                        }
                    }
                } finally {
                    Releasables.close(parsedSource);
                    context.breaker().addWithoutBreaking(-estimate);
                }
            }
            for (int c = 0; c < columns; c++) {
                built[c] = children[c].builder().build();
            }
            parents = page.filter(true, positions, 0, rows);
            Page expanded = parents.appendBlocks(built);
            parents = null;
            built = null;
            return expanded;
        } finally {
            Releasables.closeExpectNoException(children);
            if (built != null) {
                Releasables.closeExpectNoException(built);
            }
            if (parents != null) {
                parents.releaseBlocks();
            }
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return factory.describe();
    }
}
