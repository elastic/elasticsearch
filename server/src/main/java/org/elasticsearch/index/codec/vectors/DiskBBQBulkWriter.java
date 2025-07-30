/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Base class for bulk writers that write vectors to disk using the BBQ encoding.
 * This class provides the structure for writing vectors in bulk, with specific
 * implementations for different bit sizes strategies.
 */
abstract class DiskBBQBulkWriter {
    protected final int bulkSize;
    protected final IndexOutput out;

    protected DiskBBQBulkWriter(int bulkSize, IndexOutput out) {
        this.bulkSize = bulkSize;
        this.out = out;
    }

    abstract void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException;

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
        out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        int targetComponentSum = correction.quantizedComponentSum();
        assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
        out.writeShort((short) targetComponentSum);
    }

    static class OneBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        OneBitDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections, out);
            }
            // write tail
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction, out);
            }
        }
    }

    static class FourBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        FourBitDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections, out);
            }
            // write tail
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction, out);
            }
        }
    }
}
