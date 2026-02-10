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
        if (in instanceof StoreMetricsIndexInput) {
            // annoyingly, source-only snapshots do this for linked files.
            return in;
        } else if (in instanceof RandomAccessInput) {
            return new RandomAccessIndexInput(resourceDescription, in, metricHolder);
        } else {
            return new StoreMetricsIndexInput(resourceDescription, in, metricHolder);
        }
    }

    private StoreMetricsIndexInput(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        super(resourceDescription, in);
        this.metricHolder = metricHolder;
        assert in instanceof StoreMetricsIndexInput == false;
    }

    @Override
    public byte readByte() throws IOException {
        byte result = in.readByte();
        addBytesRead(1);
        return result;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readBytes(b, offset, len);
        addBytesRead(len);
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
        return createCopy(sliceDescription, in.slice(sliceDescription, offset, length), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
        return createCopy(sliceDescription, in.slice(sliceDescription, offset, length, context), metricHolder.singleThreaded());
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
        RandomAccessInput delegate = in.randomAccessSlice(offset, length);
        if (delegate instanceof IndexInput input) {
            return new RandomAccessIndexInput(input.toString(), input, metricHolder.singleThreaded());
        } else {
            return new MetricsRandomAccessInput(delegate, metricHolder.singleThreaded());
        }
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        in.prefetch(offset, length);
    }

    @Override
    public Optional<Boolean> isLoaded() {
        return in.isLoaded();
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
        in.updateIOContext(context);
    }

    void addBytesRead(long bytes) {
        metricHolder.instance().addBytesRead(bytes);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        in.readBytes(b, offset, len, useBuffer);
        addBytesRead(len);
    }

    @Override
    public short readShort() throws IOException {
        short result = in.readShort();
        addBytesRead(2);
        return result;
    }

    @Override
    public int readInt() throws IOException {
        int result = getDelegate().readInt();
        addBytesRead(4);
        return result;
    }

    @Override
    public int readVInt() throws IOException {
        long position = in.getFilePointer();
        int result = in.readVInt();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public int readZInt() throws IOException {
        long position = in.getFilePointer();
        int result = in.readZInt();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public long readLong() throws IOException {
        long result = getDelegate().readLong();
        addBytesRead(8);
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
        long position = in.getFilePointer();
        long result = in.readVLong();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public long readZLong() throws IOException {
        long position = in.getFilePointer();
        long result = in.readZLong();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);

        return result;
    }

    @Override
    public String readString() throws IOException {
        long position = in.getFilePointer();
        String result = in.readString();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
        long position = in.getFilePointer();
        Map<String, String> result = in.readMapOfStrings();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
        long position = in.getFilePointer();
        Set<String> result = in.readSetOfStrings();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
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
        public long length() {
            return delegate.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
            byte result = delegate.readByte(pos);
            addBytesRead(1);
            return result;
        }

        @Override
        public short readShort(long pos) throws IOException {
            short result = delegate.readShort(pos);
            addBytesRead(2);
            return result;
        }

        @Override
        public int readInt(long pos) throws IOException {
            int result = delegate.readInt(pos);
            addBytesRead(4);
            return result;
        }

        @Override
        public long readLong(long pos) throws IOException {
            long result = delegate.readLong(pos);
            addBytesRead(8);
            return result;
        }
    }

    private static class MetricsRandomAccessInput implements RandomAccessInput {
        private final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder;
        private final RandomAccessInput delegate;

        private MetricsRandomAccessInput(RandomAccessInput delegate, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
            this.delegate = delegate;
            this.metricHolder = metricHolder;
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
