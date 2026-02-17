/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

/**
 * Base class for bulk writers that write vectors to disk using the BBQ encoding.
 * This class provides the structure for writing vectors in bulk, with specific
 * implementations for different bit sizes strategies.
 */
public abstract sealed class DiskBBQBulkWriter {
    protected final int bulkSize;
    protected final IndexOutput out;

    protected DiskBBQBulkWriter(int bulkSize, IndexOutput out) {
        this.bulkSize = bulkSize;
        this.out = out;
    }

    /**
     * Bulk write vectors to disk. Tail vectors are not block encoded.
     * @param qvv quantized vector values
     * @param docsWriter docs writer
     * @throws IOException if writing fails
     */
    public abstract void writeVectors(QuantizedVectorValues qvv, CheckedIntConsumer<IOException> docsWriter) throws IOException;

    /**
     * Factory method to create a DiskBBQBulkWriter based on the bit size.
     * @param bitSize the bit size of the quantized vectors
     * @param bulkSize the number of vectors to write in bulk
     * @param out the IndexOutput to write to
     * @return a DiskBBQBulkWriter instance
     */
    public static DiskBBQBulkWriter fromBitSize(int bitSize, int bulkSize, IndexOutput out) {
        return fromBitSize(bitSize, bulkSize, out, false, false);
    }

    /**
     * Factory method to create a DiskBBQBulkWriter based on the bit size.
     * @param bitSize the bit size of the quantized vectors
     * @param bulkSize the number of vectors to write in bulk
     * @param out the IndexOutput to write to
     * @param blockEncodeTailVectors whether to block encode tail vectors
     * @return a DiskBBQBulkWriter instance
     */
    public static DiskBBQBulkWriter fromBitSize(
        int bitSize,
        int bulkSize,
        IndexOutput out,
        boolean blockEncodeTailVectors,
        boolean writeComponentSumAsInt
    ) {
        return switch (bitSize) {
            case 1, 2, 4 -> blockEncodeTailVectors
                ? new SmallBitEncodedDiskBBQBulkWriter(bulkSize, out, writeComponentSumAsInt)
                : new SmallBitDiskBBQBulkWriter(bulkSize, out, writeComponentSumAsInt);
            case 7 -> blockEncodeTailVectors
                ? new LargeBitEncodedDiskBBQBulkWriter(bulkSize, out)
                : new LargeBitDiskBBQBulkWriter(bulkSize, out);
            default -> throw new IllegalArgumentException("Unsupported bit size: " + bitSize);
        };
    }

    private static non-sealed class SmallBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        protected final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        private final boolean writeComponentSumAsInt;

        private SmallBitDiskBBQBulkWriter(int bulkSize, IndexOutput out, boolean writeComponentSumAsInt) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
            this.writeComponentSumAsInt = writeComponentSumAsInt;
        }

        @Override
        public void writeVectors(QuantizedVectorValues qvv, CheckedIntConsumer<IOException> docsWriter) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                if (docsWriter != null) {
                    docsWriter.accept(i);
                }
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }
            if (i < qvv.count() && docsWriter != null) {
                docsWriter.accept(i);
            }

            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction);
            }

        }

        void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections) throws IOException {
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                int targetComponentSum = correction.quantizedComponentSum();
                if (writeComponentSumAsInt) {
                    out.writeInt(targetComponentSum);
                } else {
                    assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
                    out.writeShort((short) targetComponentSum);
                }
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            }
        }

        private void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction) throws IOException {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            int targetComponentSum = correction.quantizedComponentSum();
            if (writeComponentSumAsInt) {
                out.writeInt(targetComponentSum);
            } else {
                assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
                out.writeShort((short) targetComponentSum);
            }
        }
    }

    private static non-sealed class LargeBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        protected final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        private LargeBitDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        public void writeVectors(QuantizedVectorValues qvv, CheckedIntConsumer<IOException> docsWriter) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }

            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction);
            }

        }

        void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections) throws IOException {
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(correction.quantizedComponentSum());
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            }
        }

        private void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction) throws IOException {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            out.writeInt(correction.quantizedComponentSum());
        }
    }

    private static class LargeBitEncodedDiskBBQBulkWriter extends LargeBitDiskBBQBulkWriter {

        private LargeBitEncodedDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
        }

        @Override
        public void writeVectors(QuantizedVectorValues qvv, CheckedIntConsumer<IOException> docsWriter) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }
            // write tail
            OptimizedScalarQuantizer.QuantizationResult[] tailCorrections = new OptimizedScalarQuantizer.QuantizationResult[qvv.count()
                - i];
            int j = 0;
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                tailCorrections[j] = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                j++;
            }
            writeCorrections(tailCorrections);
        }
    }

    private static class SmallBitEncodedDiskBBQBulkWriter extends SmallBitDiskBBQBulkWriter {

        private SmallBitEncodedDiskBBQBulkWriter(int bulkSize, IndexOutput out, boolean writeComponentSumAsInt) {
            super(bulkSize, out, writeComponentSumAsInt);
        }

        @Override
        public void writeVectors(QuantizedVectorValues qvv, CheckedIntConsumer<IOException> docsWriter) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                if (docsWriter != null) {
                    docsWriter.accept(i);
                }
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }
            // write tail
            if (i < qvv.count() && docsWriter != null) {
                docsWriter.accept(i);
            }
            OptimizedScalarQuantizer.QuantizationResult[] tailCorrections = new OptimizedScalarQuantizer.QuantizationResult[qvv.count()
                - i];
            int j = 0;
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                tailCorrections[j] = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                j++;
            }
            writeCorrections(tailCorrections);
        }
    }
}
