/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * Keeps aggregate summaries columnar while preserving independently absent metrics as nullable struct children.
 * <p>
 * A row is a null struct only when all four metrics are absent: {@link AggregateMetricDoubleBlock} has no separate
 * validity, so it can't distinguish a missing summary from a summary where every metric is missing. Rows with at
 * least one metric are valid structs whose absent metrics are null children.
 * <p>
 * Summaries are always single-valued, so columns of this type are never encoded as Arrow lists.
 */
class AggregateMetricDoubleArrowFormatter extends BlockArrowFormatter {
    private static final ScalarArrowFormatters.AsFloat64 DOUBLE_FORMATTER = new ScalarArrowFormatters.AsFloat64(DataType.DOUBLE);
    private static final ScalarArrowFormatters.AsInt32 COUNT_FORMATTER = new ScalarArrowFormatters.AsInt32(DataType.INTEGER);

    private final DataType esqlType;

    AggregateMetricDoubleArrowFormatter(DataType esqlType) {
        super(esqlType, Types.MinorType.STRUCT);
        this.esqlType = esqlType;
    }

    @Override
    public Field arrowField(String name) {
        var doubleType = FieldType.nullable(Types.MinorType.FLOAT8.getType());
        var countType = FieldType.nullable(Types.MinorType.INT.getType());
        return new Field(
            name,
            super.arrowField(name).getFieldType(),
            List.of(
                new Field("min", doubleType, null),
                new Field("max", doubleType, null),
                new Field("sum", doubleType, null),
                new Field("value_count", countType, null)
            )
        );
    }

    @Override
    public boolean isMultivalued(String columnName, Block block) {
        return forbidMultivalued(columnName, esqlType, block);
    }

    @Override
    public void convert(
        Block block,
        boolean asList,
        List<ArrowFieldNode> nodes,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    ) {
        assert asList == false : "aggregate_metric_double columns cannot be encoded as Arrow lists";
        AggregateMetricDoubleBlock aggregate = (AggregateMetricDoubleBlock) block;
        int positions = block.getPositionCount();
        if (summaryMayBeNull(aggregate)) {
            nodes.add(new ArrowFieldNode(positions, block.nullValuesCount()));
            accumulateVectorValidity(bufs, bufWriters, block, false);
        } else {
            // The block reports nulls if any metric may be null, but a summary is only null when all four are.
            nodes.add(new ArrowFieldNode(positions, 0));
            addEmptyValidity(bufs, bufWriters);
        }
        // Child accessors on ConstantNullBlock return the null block itself, which primitive formatters also accept.
        DOUBLE_FORMATTER.convert(aggregate.minBlock(), false, nodes, bufs, bufWriters, releasables);
        DOUBLE_FORMATTER.convert(aggregate.maxBlock(), false, nodes, bufs, bufWriters, releasables);
        DOUBLE_FORMATTER.convert(aggregate.sumBlock(), false, nodes, bufs, bufWriters, releasables);
        COUNT_FORMATTER.convert(aggregate.countBlock(), false, nodes, bufs, bufWriters, releasables);
    }

    private static boolean summaryMayBeNull(AggregateMetricDoubleBlock aggregate) {
        return aggregate.minBlock().mayHaveNulls()
            && aggregate.maxBlock().mayHaveNulls()
            && aggregate.sumBlock().mayHaveNulls()
            && aggregate.countBlock().mayHaveNulls();
    }
}
