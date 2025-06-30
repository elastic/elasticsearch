/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.packAsBinary;

/**
 * Base class for bulk writers that write vectors to disk using the BBQ encoding.
 * This class provides the structure for writing vectors in bulk, with specific
 * implementations for different bit sizes strategies.
 */
public abstract class DiskBBQBulkWriter {
    protected final int bulkSize;
    protected final OptimizedScalarQuantizer quantizer;
    protected final IndexOutput out;
    protected final FloatVectorValues fvv;

    protected DiskBBQBulkWriter(int bulkSize, OptimizedScalarQuantizer quantizer, FloatVectorValues fvv, IndexOutput out) {
        this.bulkSize = bulkSize;
        this.quantizer = quantizer;
        this.out = out;
        this.fvv = fvv;
    }

    public abstract void writeOrds(IntToIntFunction ords, int count, float[] centroid) throws IOException;

    private static void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out) throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            int targetComponentSum = correction.quantizedComponentSum();
            assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
            out.writeShort((short) targetComponentSum);
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private static void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction, IndexOutput out) throws IOException {
        out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        int targetComponentSum = correction.quantizedComponentSum();
        assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
        out.writeShort((short) targetComponentSum);
        out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
    }

    public static class OneBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final byte[] binarized;
        private final byte[] initQuantized;
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        public OneBitDiskBBQBulkWriter(int bulkSize, OptimizedScalarQuantizer quantizer, FloatVectorValues fvv, IndexOutput out) {
            super(bulkSize, quantizer, fvv, out);
            this.binarized = new byte[discretize(fvv.dimension(), 64) / 8];
            this.initQuantized = new byte[fvv.dimension()];
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        public void writeOrds(IntToIntFunction ords, int count, float[] centroid) throws IOException {
            int limit = count - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    int ord = ords.apply(i + j);
                    float[] fv = fvv.vectorValue(ord);
                    corrections[j] = quantizer.scalarQuantize(fv, initQuantized, (byte) 1, centroid);
                    packAsBinary(initQuantized, binarized);
                    out.writeBytes(binarized, binarized.length);
                }
                writeCorrections(corrections, out);
            }
            // write tail
            for (; i < count; ++i) {
                int ord = ords.apply(i);
                float[] fv = fvv.vectorValue(ord);
                OptimizedScalarQuantizer.QuantizationResult correction = quantizer.scalarQuantize(fv, initQuantized, (byte) 1, centroid);
                packAsBinary(initQuantized, binarized);
                out.writeBytes(binarized, binarized.length);
                writeCorrection(correction, out);
            }
        }
    }
}
