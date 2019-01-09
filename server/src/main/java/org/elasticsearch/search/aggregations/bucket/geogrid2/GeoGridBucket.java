/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


abstract class GeoGridBucket extends InternalMultiBucketAggregation.InternalBucket implements GeoGrid.Bucket, Comparable<GeoGridBucket> {

    // This is only used by the private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    // This field is used temporarily, and will not be used in serialization or comparison.
    long bucketOrd;

    protected long hashAsLong;
    protected long docCount;
    protected InternalAggregations aggregations;

    /**
     * Factory method to instantiate a new bucket from inside the bucket.
     * Derived buckets should do  <code>return new Bucket(hashAsLong, docCount, aggregations);</code>
     */
    protected abstract GeoGridBucket newBucket(long hashAsLong, long docCount, InternalAggregations aggregations);

    protected GeoGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.hashAsLong = hashAsLong;
    }

    /**
     * Read from a stream.
     */
    protected GeoGridBucket(StreamInput in) throws IOException {
        hashAsLong = in.readLong();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readAggregations(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hashAsLong);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    @Override
    public String getKey() {
        return getKeyAsString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        builder.endObject();
        return builder;
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
    public int compareTo(GeoGridBucket other) {
        if (this.hashAsLong > other.hashAsLong) {
            return 1;
        }
        if (this.hashAsLong < other.hashAsLong) {
            return -1;
        }
        return 0;
    }

    public GeoGridBucket reduce(List<? extends GeoGridBucket> buckets, InternalAggregation.ReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (GeoGridBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return newBucket(hashAsLong, docCount, aggs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoGridBucket bucket = (GeoGridBucket) o;
        return hashAsLong == bucket.hashAsLong &&
            docCount == bucket.docCount &&
            Objects.equals(aggregations, bucket.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashAsLong, docCount, aggregations);
    }

}
