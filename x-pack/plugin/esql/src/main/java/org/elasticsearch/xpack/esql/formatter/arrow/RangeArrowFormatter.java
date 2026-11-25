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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.function.Function;

/**
 * Formatter for ES|QL range blocks encoded as an Arrow struct with nullable {@code "from"} and {@code "to"} fields,
 * delegating child bound encoding to {@link ScalarArrowFormatters}.
 */
class RangeArrowFormatter extends BlockArrowFormatter {

    private static final ScalarArrowFormatters.AsInt64 DATE_BOUND_FORMATTER = new ScalarArrowFormatters.AsInt64(
        DataType.DATETIME,
        Types.MinorType.TIMESTAMPMILLI
    );
    private static final ScalarArrowFormatters.AsFloat64 DOUBLE_BOUND_FORMATTER = new ScalarArrowFormatters.AsFloat64(DataType.DOUBLE);

    @FunctionalInterface
    private interface BoundVectorConverter {
        void convert(Block boundBlock, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters);
    }

    private final Types.MinorType childType;
    private final Function<Block, Block> fromBlock;
    private final Function<Block, Block> toBlock;
    private final BoundVectorConverter boundConverter;

    private RangeArrowFormatter(
        DataType esqlType,
        Types.MinorType childType,
        Function<Block, Block> fromBlock,
        Function<Block, Block> toBlock,
        BoundVectorConverter boundConverter
    ) {
        super(esqlType, Types.MinorType.STRUCT);
        this.childType = childType;
        this.fromBlock = fromBlock;
        this.toBlock = toBlock;
        this.boundConverter = boundConverter;
    }

    static RangeArrowFormatter dateRange(DataType esqlType) {
        return new RangeArrowFormatter(
            esqlType,
            Types.MinorType.TIMESTAMPMILLI,
            block -> ((LongRangeBlock) block).getFromBlock(),
            block -> ((LongRangeBlock) block).getToBlock(),
            DATE_BOUND_FORMATTER::convertValueVector
        );
    }

    static RangeArrowFormatter doubleRange(DataType esqlType) {
        return new RangeArrowFormatter(
            esqlType,
            Types.MinorType.FLOAT8,
            block -> ((DoubleRangeBlock) block).getDoubleFromBlock(),
            block -> ((DoubleRangeBlock) block).getDoubleToBlock(),
            DOUBLE_BOUND_FORMATTER::convertValueVector
        );
    }

    @Override
    public Field arrowField(String name) {
        Field superField = super.arrowField(name);
        var fromField = new Field("from", FieldType.nullable(childType.getType()), null);
        var toField = new Field("to", FieldType.nullable(childType.getType()), null);
        return new Field(name, superField.getFieldType(), List.of(fromField, toField));
    }

    @Override
    public void convert(
        Block b,
        boolean asList,
        List<ArrowFieldNode> nodes,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    ) {
        addSimpleFieldNodes(b, asList, nodes);
        if (asList) {
            addListOffsets(bufs, bufWriters, b);
        }
        accumulateVectorValidity(bufs, bufWriters, b, asList);

        Block from = fromBlock.apply(b);
        Block to = toBlock.apply(b);
        assert arrowValueCount(from, asList) == arrowValueCount(b, asList)
            : "Range 'from' value count [" + arrowValueCount(from, asList) + "] differs from parent [" + arrowValueCount(b, asList) + "]";
        assert arrowValueCount(to, asList) == arrowValueCount(b, asList)
            : "Range 'to' value count [" + arrowValueCount(to, asList) + "] differs from parent [" + arrowValueCount(b, asList) + "]";
        boundConverter.convert(from, asList, nodes, bufs, bufWriters);
        boundConverter.convert(to, asList, nodes, bufs, bufWriters);
    }
}
