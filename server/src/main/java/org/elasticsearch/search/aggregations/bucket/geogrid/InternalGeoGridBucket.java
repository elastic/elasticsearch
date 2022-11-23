/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class InternalGeoGridBucket extends InternalMultiBucketAggregation.InternalBucket
    implements
        GeoGrid.Bucket,
        Comparable<InternalGeoGridBucket> {

    protected long hashAsLong;
    protected long docCount;
    protected InternalAggregations aggregations;

    long bucketOrd;

    public InternalGeoGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.hashAsLong = hashAsLong;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoGridBucket(StreamInput in) throws IOException {
        hashAsLong = in.readLong();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hashAsLong);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    public long hashAsLong() {
        return hashAsLong;
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public Aggregations getAggregations() {
        return aggregations;
    }

    @Override
    public int compareTo(InternalGeoGridBucket other) {
        if (this.hashAsLong > other.hashAsLong) {
            return 1;
        }
        if (this.hashAsLong < other.hashAsLong) {
            return -1;
        }
        return 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Aggregation.CommonFields.KEY.getPreferredName(), getKeyAsString());
        builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalGeoGridBucket bucket = (InternalGeoGridBucket) o;
        return hashAsLong == bucket.hashAsLong && docCount == bucket.docCount && Objects.equals(aggregations, bucket.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashAsLong, docCount, aggregations);
    }

}
