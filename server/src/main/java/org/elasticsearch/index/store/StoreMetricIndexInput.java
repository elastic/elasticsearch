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

public class StoreMetricIndexInput extends FilterIndexInput {
    private final MetricHolder<StoreMetrics> metricHolder;

    public StoreMetricIndexInput(String resourceDescription, IndexInput in, MetricHolder<StoreMetrics> metricHolder) {
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

    @Override
    public IndexInput clone() {
        return new StoreMetricIndexInput(toString(), in.clone(), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new StoreMetricIndexInput(sliceDescription, super.slice(sliceDescription, offset, length), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
        return new StoreMetricIndexInput(
            sliceDescription,
            super.slice(sliceDescription, offset, length, context),
            metricHolder.singleThreaded()
        );
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
        RandomAccessInput delegate = in.randomAccessSlice(offset, length);

        return new RandomAccessInput() {
            private final MetricHolder<StoreMetrics> metricHolder = StoreMetricIndexInput.this.metricHolder;

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
}
