/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rows;
import org.elasticsearch.xpack.esql.plan.logical.Rows.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.data.ElementType.fromJava;
import static org.elasticsearch.xpack.esql.planner.PlannerUtils.NON_BREAKING_BLOCK_FACTORY;

public final class ReplaceRowAsLocalRelation extends OptimizerRules.ParameterizedOptimizerRule<Rows, LogicalOptimizerContext> {
    public ReplaceRowAsLocalRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Rows rows, LogicalOptimizerContext context) {
        var entries = rows.entries();
        var output = rows.output();
        var numberOfRows = entries.size();
        Map<String, BlockBuilder> nameBlockBuilderMap = new LinkedHashMap<>();
        try {
            for (var attribute : output) {
                nameBlockBuilderMap.put(attribute.name(), new BlockBuilder(numberOfRows));
            }
            for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
                Row row = entries.get(rowIndex);
                for (var field : row.fields()) {
                    var builder = nameBlockBuilderMap.get(field.name());
                    checkType(builder, row, field);
                    builder.addValue(rowIndex, field.child().fold(context.foldCtx()));
                }
            }
            var builders = nameBlockBuilderMap.values();
            Block[] blocks = new Block[builders.size()];
            var index = 0;
            for (var builder : builders) {
                blocks[index] = builder.build();
                index++;
            }
            var page = LocalSupplier.of(blocks.length == 0 ? new Page(0) : new Page(numberOfRows, blocks));
            return new LocalRelation(rows.source(), output, page);
        } finally {
            for (var builder : nameBlockBuilderMap.values()) {
                builder.close();
            }
        }
    }

    public void checkType(BlockBuilder builder, Row row, Alias field) throws ParsingException {
        if (field.dataType() == DataType.NULL) {
            return;
        }
        if (builder.dataType == null) {
            builder.dataType = field.dataType();
        }
        if (builder.dataType.equals(field.dataType())) {
            return;
        }
        throw new ParsingException(
            field.source(),
            "Field '{}' was previously identified as of type {} but a later 'ROW {}' seems to specify it as type {}",
            field.name(),
            builder.dataType,
            row.source().text(),
            field.dataType()
        );
    }

    private static class BlockBuilder implements AutoCloseable {

        private final int finalSize;
        private BlockUtils.BuilderWrapper wrapper;
        private int index = 0;

        BlockBuilder(int finalSize) {
            this.finalSize = finalSize;
        }

        private void createWrapper(Object value) {
            var type = fromJava(value instanceof List<?> ? ((List<?>) value).get(0).getClass() : value.getClass());
            this.wrapper = BlockUtils.wrapperFor(NON_BREAKING_BLOCK_FACTORY, type, finalSize);
        }

        public void addValue(int position, Object value) {
            if (wrapper == null) {
                if (value == null) {
                    return;
                }
                createWrapper(value);
            }
            for (; index < position; index++) {
                wrapper.accept(null);
            }
            wrapper.accept(value);
            index++;
        }

        public Block build() {
            if (wrapper == null) {
                return NON_BREAKING_BLOCK_FACTORY.newConstantNullBlock(finalSize);
            }
            for (; index < finalSize; index++) {
                wrapper.accept(null);
            }
            return wrapper.builder().build();
        }

        public void close() {
            if (wrapper != null) {
                Releasables.closeExpectNoException(wrapper);
            }
        }

        /* === logically separate but safes us a map lookup === */
        public DataType dataType;
    }
}
