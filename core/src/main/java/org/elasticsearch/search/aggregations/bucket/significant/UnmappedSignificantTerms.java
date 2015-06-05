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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class UnmappedSignificantTerms extends InternalSignificantTerms<UnmappedSignificantTerms, InternalSignificantTerms.Bucket> {

    public static final Type TYPE = new Type("significant_terms", "umsigterms");

    private static final List<Bucket> BUCKETS = Collections.emptyList();
    private static final Map<String, Bucket> BUCKETS_MAP = Collections.emptyMap();

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public UnmappedSignificantTerms readResult(StreamInput in) throws IOException {
            UnmappedSignificantTerms buckets = new UnmappedSignificantTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    UnmappedSignificantTerms() {} // for serialization

    public UnmappedSignificantTerms(String name, int requiredSize, long minDocCount, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        //We pass zero for index/subset sizes because for the purpose of significant term analysis 
        // we assume an unmapped index's size is irrelevant to the proceedings. 
        super(0, 0, name, requiredSize, minDocCount, JLHScore.INSTANCE, BUCKETS, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public UnmappedSignificantTerms create(List<InternalSignificantTerms.Bucket> buckets) {
        return new UnmappedSignificantTerms(this.name, this.requiredSize, this.minDocCount, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public InternalSignificantTerms.Bucket createBucket(InternalAggregations aggregations, InternalSignificantTerms.Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    protected UnmappedSignificantTerms create(long subsetSize, long supersetSize, List<Bucket> buckets, InternalSignificantTerms prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        for (InternalAggregation aggregation : aggregations) {
            if (!(aggregation instanceof UnmappedSignificantTerms)) {
                return aggregation.reduce(aggregations, reduceContext);
            }
        }
        return this;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        this.buckets = BUCKETS;
        this.bucketMap = BUCKETS_MAP;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS).endArray();
        return builder;
    }

}
