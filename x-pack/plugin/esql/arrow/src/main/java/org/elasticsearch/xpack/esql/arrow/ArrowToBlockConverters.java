/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.arrow.BooleanArrowBlock;
import org.elasticsearch.compute.data.arrow.BytesRefArrowBlock;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Float16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.FloatToDoubleArrowBlock;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongMul1kArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt32ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt8ArrowBufBlock;

import java.util.EnumMap;
import java.util.Map;

/**
 * Registry of converters from Arrow FieldVector to ESQL Block.
 * <p>
 * Each converter handles both flat vectors and {@link ListVector} (multi-valued) inputs.
 * Most types use zero-copy ArrowBufBlock wrappers that natively understand ListVector offset
 * buffers. Types that require data transformation (e.g. FLOAT4 to double widening, BIT to boolean)
 * use copy-based converters with explicit list iteration.
 */
public class ArrowToBlockConverters {
    private static final Map<Types.MinorType, ArrowToBlockConverter> CONVERTERS = buildConverters();

    /**
     * Get a converter for the given Arrow type.
     * @param arrowType the Arrow minor type
     * @return the appropriate converter, or null if the type is not supported
     */
    static ArrowToBlockConverter forType(Types.MinorType arrowType) {
        return CONVERTERS.get(arrowType);
    }

    private static Map<Types.MinorType, ArrowToBlockConverter> buildConverters() {
        var map = new EnumMap<Types.MinorType, ArrowToBlockConverter>(Types.MinorType.class);
        map.put(Types.MinorType.FLOAT2, Float16ArrowBufBlock::of);
        map.put(Types.MinorType.FLOAT4, FloatToDoubleArrowBlock::of);
        map.put(Types.MinorType.FLOAT8, DoubleArrowBufBlock::of);
        map.put(Types.MinorType.TINYINT, Int8ArrowBufBlock::of);
        map.put(Types.MinorType.SMALLINT, Int16ArrowBufBlock::of);
        map.put(Types.MinorType.INT, IntArrowBufBlock::of);
        map.put(Types.MinorType.BIGINT, LongArrowBufBlock::of);
        map.put(Types.MinorType.UINT1, UInt8ArrowBufBlock::of);
        map.put(Types.MinorType.UINT2, UInt16ArrowBufBlock::of);
        map.put(Types.MinorType.UINT4, UInt32ArrowBufBlock::of);
        map.put(Types.MinorType.BIT, BooleanArrowBlock::of);
        map.put(Types.MinorType.VARCHAR, BytesRefArrowBlock::of);
        map.put(Types.MinorType.VARBINARY, BytesRefArrowBlock::of);
        map.put(Types.MinorType.TIMESTAMPSEC, LongMul1kArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPSECTZ, LongMul1kArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPMILLI, LongArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPMILLITZ, LongArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPMICRO, LongMul1kArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPMICROTZ, LongMul1kArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPNANO, LongArrowBufBlock::of);
        map.put(Types.MinorType.TIMESTAMPNANOTZ, LongArrowBufBlock::of);
        map.put(Types.MinorType.LIST, ArrowToBlockConverters::convertList);
        return map;
    }

    private static Block convertList(FieldVector vector, BlockFactory blockFactory) {
        ListVector listVector = (ListVector) vector;
        FieldVector child = listVector.getDataVector();
        ArrowToBlockConverter childConverter = CONVERTERS.get(child.getMinorType());
        if (childConverter == null) {
            throw new UnsupportedOperationException("Unsupported LIST element type [" + child.getMinorType() + "]");
        }
        return childConverter.convert(listVector, blockFactory);
    }
}
