/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.function.Predicate;

/** An input opened for a specific context: efficient only for the contexts its directory opens the same way. */
class ContextSpecificIndexInput extends FilterIndexInput {

    private final Predicate<IOContext> contextMatches;

    private ContextSpecificIndexInput(IndexInput in, Predicate<IOContext> contextMatches) {
        super("ContextSpecific(" + in + ")", in);
        this.contextMatches = contextMatches;
    }

    static IndexInput wrap(IndexInput in, Predicate<IOContext> contextMatches) {
        return in instanceof MemorySegmentAccessInput
            ? new MemorySegments(in, contextMatches)
            : new ContextSpecificIndexInput(in, contextMatches);
    }

    @Override
    public boolean isEfficientFor(IOContext context) {
        return contextMatches.test(context) && in.isEfficientFor(context);
    }

    // only the opened input is asked how it was opened, so clones and slices can be the delegate's
    @Override
    public IndexInput clone() {
        return in.clone();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
        return in.slice(sliceDescription, offset, length, context);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        in.prefetch(offset, length);
    }

    @Override
    public Optional<Boolean> isLoaded() {
        return in.isLoaded();
    }

    /** A mapped input, which keeps exposing its memory. */
    private static final class MemorySegments extends ContextSpecificIndexInput implements MemorySegmentAccessInput {
        private final MemorySegmentAccessInput mapped;

        private MemorySegments(IndexInput in, Predicate<IOContext> contextMatches) {
            super(in, contextMatches);
            this.mapped = (MemorySegmentAccessInput) in;
        }

        @Override
        public MemorySegment segmentSliceOrNull(long pos, long len) throws IOException {
            return mapped.segmentSliceOrNull(pos, len);
        }

        @Override
        public byte readByte(long pos) throws IOException {
            return mapped.readByte(pos);
        }

        @Override
        public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
            mapped.readBytes(pos, bytes, offset, length);
        }

        @Override
        public short readShort(long pos) throws IOException {
            return mapped.readShort(pos);
        }

        @Override
        public int readInt(long pos) throws IOException {
            return mapped.readInt(pos);
        }

        @Override
        public long readLong(long pos) throws IOException {
            return mapped.readLong(pos);
        }

        @Override
        public MemorySegments clone() {
            return new MemorySegments((IndexInput) mapped.clone(), ((ContextSpecificIndexInput) this).contextMatches);
        }
    }
}
