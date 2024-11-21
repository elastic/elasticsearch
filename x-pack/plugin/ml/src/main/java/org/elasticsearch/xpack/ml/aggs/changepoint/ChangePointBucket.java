/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.Objects;

public class ChangePointBucket extends InternalMultiBucketAggregation.InternalBucket {
    private final Object key;
    private final long docCount;
    private final InternalAggregations aggregations;

    public ChangePointBucket(Object key, long docCount, InternalAggregations aggregations) {
        this.key = key;
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    public ChangePointBucket(StreamInput in) throws IOException {
        this.key = in.readGenericValue();
        this.docCount = in.readVLong();
        this.aggregations = InternalAggregations.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(key);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public String getKeyAsString() {
        return key.toString();
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangePointBucket that = (ChangePointBucket) o;
        return docCount == that.docCount && Objects.equals(key, that.key) && Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, docCount, aggregations);
    }
}
