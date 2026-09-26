/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluator-local selection of live records and their expansion back to sample positions. It neither interprets
 * dimensions nor evaluates expressions. Dictionary identities never escape the lifetime of this batch.
 */
final class PackDimBatch implements Releasable {
    private final PackDimBlock records;
    private final IntBlock positions;

    PackDimBatch(PackDimBlock input) {
        var factory = input.blockFactory();
        var ordinal = input.asOrdinalPackDim();
        int capacity = Math.min(input.getPositionCount(), ordinal == null ? input.getPositionCount() : ordinal.getDictionarySize());
        long reservation = factory.preAdjustBreakerForInt(capacity);
        PackDimBlock selected = null;
        IntBlock mapping = null;
        boolean success = false;
        try (
            IntArray seen = factory.bigArrays().newIntArray(ordinal == null ? capacity : ordinal.getDictionarySize(), true);
            var builder = factory.newIntBlockBuilder(input.getPositionCount())
        ) {
            int[] firstPositions = new int[capacity];
            int count = 0;
            for (int p = 0; p < input.getPositionCount(); p++) {
                if (input.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int identity = ordinal == null ? p : ordinal.getOrdinalsBlock().getInt(ordinal.getOrdinalsBlock().getFirstValueIndex(p));
                int record = seen.get(identity);
                if (record == 0) {
                    firstPositions[count] = p;
                    record = ++count;
                    seen.set(identity, record);
                }
                builder.appendInt(record - 1);
            }
            selected = input.filter(false, firstPositions, 0, count);
            mapping = builder.build();
            records = selected;
            positions = mapping;
            success = true;
        } finally {
            factory.adjustBreaker(-reservation);
            if (success == false) Releasables.close(selected, mapping);
        }
    }

    /** Borrowed compact input; ordinary evaluators see one position per live record. */
    PackDimBlock records() {
        return records;
    }

    /** Borrows a result in record order and returns an independently owned sample-domain block. */
    Block expand(Block result) {
        if (result.getPositionCount() != records.getPositionCount()) {
            throw new IllegalArgumentException("record result changed position count");
        }
        return expand(result, positions);
    }

    /** Expands an owned-by-caller result through a borrowed, nullable position map. */
    static Block expand(Block result, IntBlock positions) {
        var factory = result.blockFactory();
        if (result.areAllValuesNull()) return factory.newConstantNullBlock(positions.getPositionCount());
        if (result instanceof PackDimBlock packed && packed.asOrdinalPackDim() != null) {
            var ordinal = packed.asOrdinalPackDim();
            try (var builder = factory.newIntBlockBuilder(positions.getPositionCount())) {
                for (int p = 0; p < positions.getPositionCount(); p++) {
                    if (positions.isNull(p)) builder.appendNull();
                    else {
                        int record = positions.getInt(positions.getFirstValueIndex(p));
                        builder.copyFrom(ordinal.getOrdinalsBlock(), record, record + 1);
                    }
                }
                return ordinal.withOrdinals(builder.build());
            }
        }
        if (result instanceof BytesRefBlock bytes && bytes.asVector() != null) {
            var vector = bytes.asVector();
            positions.incRef();
            vector.incRef();
            return new OrdinalBytesRefBlock(positions, vector);
        }
        try (var builder = result.elementType().newBlockBuilder(positions.getPositionCount(), factory)) {
            for (int p = 0; p < positions.getPositionCount(); p++) {
                if (positions.isNull(p)) builder.appendNull();
                else {
                    int record = positions.getInt(positions.getFirstValueIndex(p));
                    builder.copyFrom(result, record, record + 1);
                }
            }
            return builder.build();
        }
    }

    @Override
    public void close() {
        Releasables.close(records, positions);
    }
}
