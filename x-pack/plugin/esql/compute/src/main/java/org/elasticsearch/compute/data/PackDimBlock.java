/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * A column of immutable named dimension values. Names belong to the values, not the page schema.
 * Access follows the usual Block position/value-index contract. An empty value is not a null row,
 * and null-valued dimensions remain present until explicitly unset.
 */
public sealed interface PackDimBlock extends Block permits ConstantNullBlock, OrdinalPackDimBlock {
    TransportVersion ESQL_PACK_DIM = TransportVersion.fromName("esql_pack_dim_buf");

    /**
     * Populates a borrowed view of one non-null value. Call isNull(position) before accessing its value index.
     * The scratch holder and its bytes may be reused; keep this block open while reading them.
     */
    PackDimValue getPackDim(int valueIndex, PackDimValue scratch);

    /** Optional dictionary fast path, analogous to ordinal byte blocks. IDs are local to this block. */
    default OrdinalPackDimBlock asOrdinalPackDim() {
        return null;
    }

    /** Construction uses the same builder boundary for explicit dimensions and runtime readers. */
    interface Builder extends Block.Builder, BlockLoader.PackDimBuilder {
        /** Copies the borrowed value; neither its holder nor its bytes are retained by reference. */
        Builder appendPackDim(PackDimValue value);

        @Override
        Builder append(BytesRef[] names, BytesRef[] values);

        @Override
        Builder appendNull();

        @Override
        Builder copyFrom(Block block, int begin, int end);

        @Override
        PackDimBlock build();
    }

    @Override
    PackDimBlock slice(int begin, int end);

    @Override
    PackDimBlock filter(boolean duplicates, int[] positions, int offset, int length);

    @Override
    PackDimBlock keepMask(BooleanVector mask);

    @Override
    PackDimBlock deepCopy(BlockFactory factory);
}
