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
import org.elasticsearch.compute.data.arrow.Float32ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongMul1kArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt32ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt8ArrowBufBlock;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

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
     * Arrow LIST element types whose runtime conversion preserves ESQL's multi-value invariants
     * (null lists, empty lists, and null children all collapse to an ESQL null position).
     * <p>
     * This is the subset for which list-aware copy converters exist today
     * ({@link BooleanArrowBlock}, {@link BytesRefArrowBlock}).
     * Other registered converters (e.g. {@link IntArrowBufBlock}, {@link LongArrowBufBlock},
     * {@link Float32ArrowBufBlock}) use the zero-copy {@code AbstractArrowBufBlock} list path,
     * which throws on null children and yields malformed blocks for empty lists in non-null
     * positions; until copy fallbacks (or a try-zero-copy-then-fallback dispatch) are in place
     * for those types, LIST inputs of those element types are rejected at conversion and at
     * schema-inference time.
     * <p>
     * TODO: extend list support to fixed-width int/long/double/timestamp/float children. Likely
     * shape is a per-call pre-screen (child has no nulls AND no empty list in a non-null
     * position) that dispatches to the existing zero-copy converters when safe and to new copy
     * fallbacks otherwise.
     */
    private static final Set<Types.MinorType> SUPPORTED_LIST_CHILD_TYPES = EnumSet.of(
        Types.MinorType.BIT,
        Types.MinorType.VARCHAR,
        Types.MinorType.VARBINARY
    );

    /**
     * Get a converter for the given Arrow type.
     * @param arrowType the Arrow minor type
     * @return the appropriate converter, or null if the type is not supported
     */
    static ArrowToBlockConverter forType(Types.MinorType arrowType) {
        return CONVERTERS.get(arrowType);
    }

    static boolean isListChildTypeSupported(Types.MinorType childType) {
        return SUPPORTED_LIST_CHILD_TYPES.contains(childType);
    }

    private static Map<Types.MinorType, ArrowToBlockConverter> buildConverters() {
        var map = new EnumMap<Types.MinorType, ArrowToBlockConverter>(Types.MinorType.class);
        map.put(Types.MinorType.FLOAT2, Float16ArrowBufBlock::of);
        map.put(Types.MinorType.FLOAT4, Float32ArrowBufBlock::of);
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
        Types.MinorType childType = listVector.getDataVector().getMinorType();
        if (isListChildTypeSupported(childType) == false) {
            throw new UnsupportedOperationException(
                "LIST<" + childType + "> is not supported; supported child types are " + SUPPORTED_LIST_CHILD_TYPES
            );
        }
        // Non-null by construction: SUPPORTED_LIST_CHILD_TYPES is a subset of the registered converter keys.
        return CONVERTERS.get(childType).convert(listVector, blockFactory);
    }
}
