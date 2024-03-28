/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;

import java.io.IOException;

/**
 * Utility class to compress integers on heap using PFOR ({@link PForUtil}) compressing technique. The API is
 * based on lucene`s {@link org.apache.lucene.util.packed.PackedLongValues}
 * */
public class PForLongValues {
    private final PForUtil pForUtil = new PForUtil(new ForUtil());
    private final long[] buffer = new long[ForUtil.BLOCK_SIZE];
    private final ByteBuffersDataOutput output = new ByteBuffersDataOutput();

    private int totalCount;

    private PForLongValues() {}

    /** Returns an empty builder instance */
    public static Builder pForBuilder() {
        PForLongValues encoder = new PForLongValues();
        return encoder.builder();
    }

    private Builder builder() {
        return new Builder();
    }

    /** Get the number of values in this array. */
    public int size() {
        return totalCount;
    }

    /** Return an iterator over the values of this array. */
    public Iterator iterator() throws IOException {
        return new Iterator(output.toDataInput());
    }

    /** A Builder for a {@link PForLongValues} instance. */
    public final class Builder {

        private boolean finished = false;
        private int bufferCount;

        private Builder() {}

        /** Add a new element to this builder. */
        public void add(long val) throws IOException {
            if (finished) {
                throw new IllegalStateException("Cannot be used after build()");
            }
            buffer[bufferCount++] = val;
            if (bufferCount == ForUtil.BLOCK_SIZE) {
                totalCount += bufferCount;
                pForUtil.encode(buffer, output);
                bufferCount = 0;
            }
        }

        /** Get the number of values in this array. */
        public int size() {
            return totalCount + bufferCount;
        }

        /**
         * Build a {@link PForLongValues} instance that contains values that have been added to this
         * builder. This operation is destructive.
         */
        public PForLongValues build() throws IOException {
            if (finished) {
                throw new IllegalStateException("Already build");
            }
            finished = true;
            totalCount += bufferCount;
            for (int i = 0; i < bufferCount; i++) {
                output.writeVLong(buffer[i]);
            }
            return PForLongValues.this;
        }
    }

    /** An iterator over long values. */
    public final class Iterator {
        int vOff, pOff;
        int currentCount; // number of entries of the current page
        private final DataInput input;

        private Iterator(DataInput input) throws IOException {
            this.input = input;
            vOff = pOff = 0;
            fillBlock();
        }

        private void fillBlock() throws IOException {
            if (vOff == totalCount) {
                currentCount = 0;
            } else {
                int remaining = totalCount - vOff;
                if (remaining >= ForUtil.BLOCK_SIZE) {
                    pForUtil.decode(input, buffer);
                    vOff += ForUtil.BLOCK_SIZE;
                    currentCount = ForUtil.BLOCK_SIZE;
                } else {
                    for (int i = 0; i < remaining; i++) {
                        buffer[i] = input.readVLong();
                    }
                    vOff = totalCount;
                    currentCount = remaining;
                }
                assert currentCount > 0;
            }
        }

        /** Whether or not there are remaining values. */
        public boolean hasNext() {
            return pOff < currentCount;
        }

        /** Return the next long in the buffer. */
        public long next() throws IOException {
            assert hasNext();
            long result = buffer[pOff++];
            if (pOff == currentCount) {
                pOff = 0;
                fillBlock();
            }
            return result;
        }
    }
}
