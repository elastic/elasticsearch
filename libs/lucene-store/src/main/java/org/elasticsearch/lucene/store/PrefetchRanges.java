/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects what a stage asks for through the inputs it {@link #wrap}s and sends it when the stage ends, merging ranges in
 * the same or adjacent read units, which asks for no unit that none of them touches.
 */
public final class PrefetchRanges {

    /** The read unit of the running stage. */
    private int readUnit = 4096;

    /** The {@code from, to} ranges asked for since the last flush, by input. */
    private final Map<IndexInput, List<long[]>> pending = new IdentityHashMap<>();

    /** An input reading as {@code in} whose prefetches are collected; {@code in} itself if it is one already. */
    public IndexInput wrap(IndexInput in) {
        if (in instanceof Recording recording && recording.owner() == this) {
            return in;
        }
        return new Recording(in, in, 0);
    }

    /** Sets the read unit of the stage about to run. */
    void readUnit(int readUnit) {
        this.readUnit = readUnit;
    }

    /** Whether {@code [offset, offset + length)} lies within a single read unit. */
    public boolean withinOneUnit(long offset, long length) {
        return length > 0 && offset / readUnit == (offset + length - 1) / readUnit;
    }

    /** Sends the ranges collected since the last flush. */
    public void flush() throws IOException {
        for (Map.Entry<IndexInput, List<long[]>> entry : pending.entrySet()) {
            List<long[]> ranges = entry.getValue();
            if (ranges.isEmpty()) {
                continue;
            }
            ranges.sort((a, b) -> Long.compare(a[0], b[0]));
            long from = ranges.get(0)[0];
            long to = ranges.get(0)[1];
            for (int i = 1; i < ranges.size(); i++) {
                long[] range = ranges.get(i);
                // Merged when it starts no later than the unit after the last one the run touches.
                if (range[0] / readUnit <= (to - 1) / readUnit + 1) {
                    to = Math.max(to, range[1]);
                } else {
                    entry.getKey().prefetch(from, to - from);
                    from = range[0];
                    to = range[1];
                }
            }
            entry.getKey().prefetch(from, to - from);
            ranges.clear();
        }
    }

    private void add(IndexInput root, long offset, long length) {
        if (length > 0) {
            pending.computeIfAbsent(root, k -> new ArrayList<>()).add(new long[] { offset, offset + length });
        }
    }

    /** Reads as {@code in}; collects its prefetches against {@code root}, {@code base} bytes in. */
    private final class Recording extends IndexInput {
        private final IndexInput in;
        private final IndexInput root;
        private final long base;

        Recording(IndexInput in, IndexInput root, long base) {
            super("prefetch ranges(" + in + ")");
            this.in = in;
            this.root = root;
            this.base = base;
        }

        PrefetchRanges owner() {
            return PrefetchRanges.this;
        }

        @Override
        public void prefetch(long offset, long length) {
            add(root, base + offset, length);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new Recording(in.slice(sliceDescription, offset, length), root, base + offset);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            return in.randomAccessSlice(offset, length);
        }

        @Override
        public IndexInput clone() {
            return new Recording(in.clone(), root, base);
        }

        @Override
        public byte readByte() throws IOException {
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            in.readBytes(b, offset, len);
        }

        @Override
        public short readShort() throws IOException {
            return in.readShort();
        }

        @Override
        public int readInt() throws IOException {
            return in.readInt();
        }

        @Override
        public long readLong() throws IOException {
            return in.readLong();
        }

        @Override
        public void readLongs(long[] dst, int offset, int length) throws IOException {
            in.readLongs(dst, offset, length);
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
