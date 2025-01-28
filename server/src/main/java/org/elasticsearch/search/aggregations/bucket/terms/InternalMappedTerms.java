/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common superclass for results of the terms aggregation on mapped fields.
 */
public abstract class InternalMappedTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> extends InternalTerms<A, B> {
    protected final DocValueFormat format;
    protected final int shardSize;
    protected final boolean showTermDocCountError;
    protected final long otherDocCount;
    protected final List<B> buckets;
    protected Map<String, B> bucketMap;

    protected Long docCountError;

    protected InternalMappedTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<B> buckets,
        Long docCountError
    ) {
        super(name, reduceOrder, order, requiredSize, minDocCount, metadata);
        this.format = format;
        this.shardSize = shardSize;
        this.showTermDocCountError = showTermDocCountError;
        this.otherDocCount = otherDocCount;
        this.docCountError = docCountError;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    protected InternalMappedTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in);
        if (in.readBoolean()) {
            docCountError = in.readZLong();
        } else {
            docCountError = null;
        }
        format = in.readNamedWriteable(DocValueFormat.class);
        shardSize = readSize(in);
        showTermDocCountError = in.readBoolean();
        otherDocCount = in.readVLong();
        buckets = in.readCollectionAsList(stream -> bucketReader.read(stream, format, showTermDocCountError));
    }

    @Override
    protected final void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        if (docCountError != null) {
            out.writeBoolean(true);
            out.writeZLong(docCountError);
        } else {
            out.writeBoolean(false);
        }
        out.writeNamedWriteable(format);
        writeSize(shardSize, out);
        out.writeBoolean(showTermDocCountError);
        out.writeVLong(otherDocCount);
        out.writeVInt(buckets.size());
        for (var bucket : buckets) {
            bucket.writeTo(out, showTermDocCountError);
        }
    }

    @Override
    protected void setDocCountError(long docCountError) {
        this.docCountError = docCountError;
    }

    @Override
    protected boolean getShowDocCountError() {
        return showTermDocCountError;
    }

    @Override
    protected int getShardSize() {
        return shardSize;
    }

    @Override
    public Long getDocCountError() {
        return docCountError;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalMappedTerms<?, ?> that = (InternalMappedTerms<?, ?>) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(format, that.format)
            && Objects.equals(otherDocCount, that.otherDocCount)
            && Objects.equals(showTermDocCountError, that.showTermDocCountError)
            && Objects.equals(shardSize, that.shardSize)
            && Objects.equals(docCountError, that.docCountError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, otherDocCount, showTermDocCountError, shardSize);
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, showTermDocCountError, docCountError, otherDocCount, buckets);
    }
}
