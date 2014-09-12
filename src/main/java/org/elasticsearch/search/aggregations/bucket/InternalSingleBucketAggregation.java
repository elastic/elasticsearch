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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A base class for all the single bucket aggregations.
 */
public abstract class InternalSingleBucketAggregation extends InternalAggregation implements SingleBucketAggregation {

    private long docCount;
    private InternalAggregations aggregations;

    protected InternalSingleBucketAggregation() {} // for serialization

    /**
     * Creates a single bucket aggregation.
     *
     * @param name          The aggregation name.
     * @param docCount      The document count in the single bucket.
     * @param aggregations  The already built sub-aggregations that are associated with the bucket.
     */
    protected InternalSingleBucketAggregation(String name, long docCount, InternalAggregations aggregations) {
        super(name);
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    /**
     * Create a <b>new</b> empty sub aggregation. This must be a new instance on each call.
     */
    protected abstract InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations);

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        long docCount = 0L;
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            assert aggregation.getName().equals(getName());
            docCount += ((InternalSingleBucketAggregation) aggregation).docCount;
            subAggregationsList.add(((InternalSingleBucketAggregation) aggregation).aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(subAggregationsList, reduceContext);
        return newAggregation(getName(), docCount, aggs);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readAggregations(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT, docCount);
        aggregations.toXContentInternal(builder, params);
        return builder;
    }
}
