/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exponentialhistogram.AbstractExponentialHistogram;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;

/**
 * A wrapper around ExponentialHistogram to make it writeable to @link StreamOutput}
 * so that it can be used e.g. in {@link org.elasticsearch.xpack.esql.core.expression.Literal}s.
 * Only intended for testing purposes.
 */
public class WriteableExponentialHistogram extends AbstractExponentialHistogram implements GenericNamedWriteable {

    // TODO(b/133393): as it turns out, this is also required in production. Therefore we have to properly register this class,
    // like in https://github.com/elastic/elasticsearch/pull/135054

    private static final String WRITEABLE_NAME = "test_exponential_histogram";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        GenericNamedWriteable.class,
        WRITEABLE_NAME,
        WriteableExponentialHistogram::readFrom
    );

    private final ExponentialHistogram delegate;

    public WriteableExponentialHistogram(ExponentialHistogram delegate) {
        this.delegate = delegate;
    }

    @Override
    public int scale() {
        return delegate.scale();
    }

    @Override
    public ZeroBucket zeroBucket() {
        return delegate.zeroBucket();
    }

    @Override
    public Buckets positiveBuckets() {
        return delegate.positiveBuckets();
    }

    @Override
    public Buckets negativeBuckets() {
        return delegate.negativeBuckets();
    }

    @Override
    public double sum() {
        return delegate.sum();
    }

    @Override
    public long valueCount() {
        return delegate.valueCount();
    }

    @Override
    public double min() {
        return delegate.min();
    }

    @Override
    public double max() {
        return delegate.max();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return true;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "must not be called when overriding supportsVersion";
        throw new UnsupportedOperationException("must not be called when overriding supportsVersion");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) scale());
        out.writeDouble(sum());
        out.writeDouble(min());
        out.writeDouble(max());
        out.writeDouble(zeroBucket().zeroThreshold());
        out.writeLong(zeroBucket().count());
        writeBuckets(out, negativeBuckets());
        writeBuckets(out, positiveBuckets());
    }

    private static void writeBuckets(StreamOutput out, Buckets buckets) throws IOException {
        int count = 0;
        BucketIterator iterator = buckets.iterator();
        while (iterator.hasNext()) {
            count++;
            iterator.advance();
        }
        out.writeInt(count);
        iterator = buckets.iterator();
        while (iterator.hasNext()) {
            out.writeLong(iterator.peekIndex());
            out.writeLong(iterator.peekCount());
            iterator.advance();
        }
    }

    private static WriteableExponentialHistogram readFrom(StreamInput in) throws IOException {
        byte scale = in.readByte();
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(scale, ExponentialHistogramCircuitBreaker.noop());
        builder.sum(in.readDouble());
        builder.min(in.readDouble());
        builder.max(in.readDouble());
        builder.zeroBucket(ZeroBucket.create(in.readDouble(), in.readLong()));
        int negBucketCount = in.readInt();
        for (int i = 0; i < negBucketCount; i++) {
            builder.setNegativeBucket(in.readLong(), in.readLong());
        }
        int posBucketCount = in.readInt();
        for (int i = 0; i < posBucketCount; i++) {
            builder.setPositiveBucket(in.readLong(), in.readLong());
        }
        return new WriteableExponentialHistogram(builder.build());
    }
}
