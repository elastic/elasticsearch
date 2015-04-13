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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class UnmappedTerms extends InternalTerms {

    public static final Type TYPE = new Type("terms", "umterms");

    private static final List<Bucket> BUCKETS = Collections.emptyList();
    private static final Map<String, Bucket> BUCKETS_MAP = Collections.emptyMap();

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public UnmappedTerms readResult(StreamInput in) throws IOException {
            UnmappedTerms buckets = new UnmappedTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    UnmappedTerms() {} // for serialization

    public UnmappedTerms(String name, Terms.Order order, int requiredSize, int shardSize, long minDocCount, Map<String, Object> metaData) {
        super(name, order, requiredSize, shardSize, minDocCount, BUCKETS, false, 0, 0, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.docCountError = 0;
        this.order = InternalOrder.Streams.readOrder(in);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        this.buckets = BUCKETS;
        this.bucketMap = BUCKETS_MAP;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        for (InternalAggregation agg : aggregations) {
            if (!(agg instanceof UnmappedTerms)) {
                return agg.reduce(aggregations, reduceContext);
            }
        }
        return this;
    }

    @Override
    protected InternalTerms newAggregation(String name, List<Bucket> buckets, boolean showTermDocCountError, long docCountError, long otherDocCount, Map<String, Object> metaData) {
        throw new UnsupportedOperationException("How did you get there?");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME, docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS, 0);
        builder.startArray(CommonFields.BUCKETS).endArray();
        return builder;
    }

}
