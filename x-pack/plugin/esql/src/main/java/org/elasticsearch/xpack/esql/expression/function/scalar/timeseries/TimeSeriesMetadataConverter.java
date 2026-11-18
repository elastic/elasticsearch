/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Loads {@code _timeseries} as a typed label container. The field loads as the JSON object of a series' dimensions, as it does
 * for a keyword column; this converts it, as it is read, to the label container every label container function reads (see
 * {@link TimeSeriesMetadataEncoding}), dictionary encoded: the block holds each distinct label set once and an ordinal per row.
 * A series has a row per time bucket, so its label set converts, hashes and compares once rather than once per row.
 * <p>
 * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
 * container, and a time-series aggregate keyed by it merges their samples. Prometheus has no empty label, so data written by
 * it never holds such a pair; data written otherwise may, and answers then differ from what each series alone gives. Not
 * supported for now.
 */
public final class TimeSeriesMetadataConverter implements ValuesSourceReaderOperator.ConverterFactory {
    public static final TimeSeriesMetadataConverter INSTANCE = new TimeSeriesMetadataConverter();

    private TimeSeriesMetadataConverter() {}

    @Override
    public ValuesSourceReaderOperator.ConverterEvaluator build(DriverContext context) {
        BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(context.breaker(), "load_timeseries");
        return new ValuesSourceReaderOperator.ConverterEvaluator() {
            @Override
            public Block convert(Block block) {
                try (block) {
                    return TimeSeriesMetadataConverter.convert(context, (BytesRefBlock) block, scratch);
                }
            }

            @Override
            public void close() {
                Releasables.close(scratch);
            }

            @Override
            public String toString() {
                return "TimeSeriesMetadataConverter";
            }
        };
    }

    /** {@code source}, each value the JSON object of a series' dimensions, as dictionary encoded label containers. */
    static OrdinalBytesRefBlock convert(DriverContext context, BytesRefBlock source, BreakingBytesRefBuilder scratch) {
        OrdinalBytesRefBlock loaded = source.asOrdinals();
        if (loaded != null) {
            // Already dictionary encoded: each entry converts once and the ordinals stay.
            IntBlock ordinals = loaded.getOrdinalsBlock();
            ordinals.incRef();
            return dictionaryEncoded(ordinals, convertEach(context.blockFactory(), loaded.getDictionaryVector(), scratch));
        }
        BytesRef value = new BytesRef();
        try (
            BytesRefHash distinct = new BytesRefHash(1, context.bigArrays());
            IntBlock.Builder ordinals = context.blockFactory().newIntBlockBuilder(source.getPositionCount())
        ) {
            for (int p = 0; p < source.getPositionCount(); p++) {
                if (source.isNull(p)) {
                    ordinals.appendNull();
                } else {
                    long ordinal = distinct.add(source.getBytesRef(source.getFirstValueIndex(p), value));
                    ordinals.appendInt((int) (ordinal < 0 ? -1 - ordinal : ordinal));
                }
            }
            BytesRefVector dictionary;
            try (BytesRefVector.Builder converted = context.blockFactory().newBytesRefVectorBuilder((int) distinct.size())) {
                for (long ordinal = 0; ordinal < distinct.size(); ordinal++) {
                    converted.appendBytesRef(TimeSeriesMetadataEncoding.fromSource(distinct.get(ordinal, value), scratch));
                }
                dictionary = converted.build();
            }
            IntBlock built = null;
            try {
                built = ordinals.build();
            } finally {
                if (built == null) {
                    Releasables.closeExpectNoException(dictionary);
                }
            }
            return dictionaryEncoded(built, dictionary);
        }
    }

    private static BytesRefVector convertEach(BlockFactory blockFactory, BytesRefVector sources, BreakingBytesRefBuilder scratch) {
        BytesRef value = new BytesRef();
        try (BytesRefVector.Builder converted = blockFactory.newBytesRefVectorBuilder(sources.getPositionCount())) {
            for (int i = 0; i < sources.getPositionCount(); i++) {
                converted.appendBytesRef(TimeSeriesMetadataEncoding.fromSource(sources.getBytesRef(i, value), scratch));
            }
            return converted.build();
        }
    }

    /** The block of {@code ordinals} into {@code dictionary}, both released if it cannot be built. */
    private static OrdinalBytesRefBlock dictionaryEncoded(IntBlock ordinals, BytesRefVector dictionary) {
        OrdinalBytesRefBlock result = null;
        try {
            result = new OrdinalBytesRefBlock(ordinals, dictionary);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, dictionary);
            }
        }
    }
}
