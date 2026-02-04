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
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StoreMetricsIndexInput extends FilterIndexInput {
    final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder;

    public static IndexInput create(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        if (in instanceof RandomAccessInput) {
            return new RandomAccessIndexInput(resourceDescription, in, metricHolder);
        } else {
            return new StoreMetricsIndexInput(resourceDescription, in, metricHolder);
        }
    }

    private StoreMetricsIndexInput(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        super(resourceDescription, in);
        this.metricHolder = metricHolder;
    }

    @Override
    public byte readByte() throws IOException {
        byte result = super.readByte();
        metricHolder.instance().addBytesRead(1);
        return result;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        super.readBytes(b, offset, len);
        metricHolder.instance().addBytesRead(len);
    }

    IndexInput createCopy(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        return new StoreMetricsIndexInput(resourceDescription, in, metricHolder);
    }

    @Override
    public IndexInput clone() {
        return createCopy(toString(), in.clone(), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return createCopy(sliceDescription, super.slice(sliceDescription, offset, length), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
        return createCopy(sliceDescription, super.slice(sliceDescription, offset, length, context), metricHolder.singleThreaded());
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
        RandomAccessInput delegate = in.randomAccessSlice(offset, length);

        return new RandomAccessInput() {
            private final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder = StoreMetricsIndexInput.this.metricHolder
                .singleThreaded();

            @Override
            public long length() {
                return delegate.length();
            }

            @Override
            public byte readByte(long pos) throws IOException {
                byte result = delegate.readByte(pos);
                metricHolder.instance().addBytesRead(1);
                return result;
            }

            @Override
            public short readShort(long pos) throws IOException {
                short result = delegate.readShort(pos);
                metricHolder.instance().addBytesRead(2);
                return result;
            }

            @Override
            public int readInt(long pos) throws IOException {
                int result = delegate.readInt(pos);
                metricHolder.instance().addBytesRead(4);
                return result;
            }

            @Override
            public long readLong(long pos) throws IOException {
                long result = delegate.readLong(pos);
                metricHolder.instance().addBytesRead(8);
                return result;
            }
        };
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        getDelegate().prefetch(offset, length);
    }

    @Override
    public Optional<Boolean> isLoaded() {
        return getDelegate().isLoaded();
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
        getDelegate().updateIOContext(context);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        getDelegate().readBytes(b, offset, len, useBuffer);
        metricHolder.instance().addBytesRead(len);
    }

    @Override
    public short readShort() throws IOException {
        short result = getDelegate().readShort();
        metricHolder.instance().addBytesRead(2);
        return result;
    }

    @Override
    public int readInt() throws IOException {
        int result = getDelegate().readInt();
        metricHolder.instance().addBytesRead(4);
        return result;
    }

    @Override
    public int readVInt() throws IOException {
        // keep as is for now due to variable length
        return super.readVInt();
    }

    @Override
    public int readZInt() throws IOException {
        // keep as is for now due to variable length
        return super.readZInt();
    }

    @Override
    public long readLong() throws IOException {
        long result = getDelegate().readLong();
        metricHolder.instance().addBytesRead(8);
        return result;
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        getDelegate().readLongs(dst, offset, length);
        metricHolder.instance().addBytesRead(8L * length);
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        getDelegate().readInts(dst, offset, length);
        metricHolder.instance().addBytesRead(4L * length);
    }

    @Override
    public void readFloats(float[] floats, int offset, int len) throws IOException {
        getDelegate().readFloats(floats, offset, len);
        metricHolder.instance().addBytesRead(4L * len);
    }

    @Override
    public long readVLong() throws IOException {
        // keep as is for now due to variable length
        return super.readVLong();
    }

    @Override
    public long readZLong() throws IOException {
        // keep as is for now due to variable length
        return super.readZLong();
    }

    @Override
    public String readString() throws IOException {
        // keep as is for now due to variable length
        return super.readString();
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
        // keep as is for now due to variable length
        return super.readMapOfStrings();
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
        // keep as is for now due to variable length
        return super.readSetOfStrings();
    }

    private static class RandomAccessIndexInput extends StoreMetricsIndexInput implements RandomAccessInput {
        private final RandomAccessInput delegate;

        private RandomAccessIndexInput(
            String resourceDescription,
            IndexInput in,
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder
        ) {
            super(resourceDescription, in, metricHolder);
            assert in instanceof RandomAccessInput;
            this.delegate = (RandomAccessInput) in;
        }

        @Override
        IndexInput createCopy(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
            return new RandomAccessIndexInput(resourceDescription, in, metricHolder);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            return super.randomAccessSlice(offset, length);
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
            byte result = delegate.readByte(pos);
            metricHolder.instance().addBytesRead(1);
            return result;
        }

        @Override
        public short readShort(long pos) throws IOException {
            short result = delegate.readShort(pos);
            metricHolder.instance().addBytesRead(2);
            return result;
        }

        @Override
        public int readInt(long pos) throws IOException {
            int result = delegate.readInt(pos);
            metricHolder.instance().addBytesRead(4);
            return result;
        }

        @Override
        public long readLong(long pos) throws IOException {
            long result = delegate.readLong(pos);
            metricHolder.instance().addBytesRead(8);
            return result;
        }
    }
}
