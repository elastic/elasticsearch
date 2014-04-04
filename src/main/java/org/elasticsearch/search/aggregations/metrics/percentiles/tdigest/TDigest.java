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
package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesEstimator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;


public class TDigest extends PercentilesEstimator {

    public final static byte ID = 0;

    private final BigArrays bigArrays;
    private ObjectArray<TDigestState> states;
    private final double compression;

    public TDigest(double[] percents, double compression, long estimatedBucketsCount, AggregationContext context) {
        super(percents);
        bigArrays = context.bigArrays();
        states = bigArrays.newObjectArray(estimatedBucketsCount);
        this.compression = compression;
    }

    @Override
    public void close() throws ElasticsearchException {
        states.close();
    }

    public void offer(double value, long bucketOrd) {
        states = bigArrays.grow(states, bucketOrd + 1);
        TDigestState state = states.get(bucketOrd);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucketOrd, state);
        }
        state.add(value);
    }

    @Override
    public PercentilesEstimator.Result result(long bucketOrd) {
        if (bucketOrd >= states.size() || states.get(bucketOrd) == null) {
            return emptyResult();
        }
        return new Result(percents, states.get(bucketOrd));
    }

    @Override
    public PercentilesEstimator.Result emptyResult() {
        return new Result(percents, new TDigestState(compression));
    }

    public static class Result extends PercentilesEstimator.Result<TDigest, Result> {

        private TDigestState state;

        public Result() {} // for serialization

        public Result(double[] percents, TDigestState state) {
            super(percents);
            this.state = state;
        }

        @Override
        protected byte id() {
            return ID;
        }

        @Override
        public double estimate(int index) {
            return state.quantile(percents[index] / 100);
        }

        @Override
        public Merger merger(int estimatedMerges) {
            return new Merger();
        }

        public static Result read(StreamInput in) throws IOException {
            Result result = new Result();
            result.readFrom(in);
            return result;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.percents = new double[in.readInt()];
            for (int i = 0; i < percents.length; i++) {
                percents[i] = in.readDouble();
            }
            state = TDigestState.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(percents.length);
            for (int i = 0 ; i < percents.length; ++i) {
                out.writeDouble(percents[i]);
            }
            TDigestState.write(state, out);
        }

        private class Merger implements PercentilesEstimator.Result.Merger<TDigest, Result> {

            private Result merged;

            @Override
            public void add(Result result) {
                if (merged == null || merged.state == null) {
                    merged = result;
                    return;
                }
                if (result.state == null || result.state.size() == 0) {
                    return;
                }
                merged.state.add(result.state);
            }

            @Override
            public Result merge() {
                return merged;
            }
        }

    }

    public static class Factory implements PercentilesEstimator.Factory {

        private final double compression;

        public Factory(Map<String, Object> settings) {
            double compression = 100;
            if (settings != null) {
                Object compressionObject = settings.get("compression");
                if (compressionObject != null) {
                    if (!(compressionObject instanceof Number)) {
                        throw new ElasticsearchIllegalArgumentException("tdigest compression must be number, got a " + compressionObject.getClass());
                    }
                    compression = ((Number) compressionObject).doubleValue();
                }
            }
            this.compression = compression;
        }

        public TDigest create(double[] percents, long estimtedBucketCount, AggregationContext context) {
            return new TDigest(percents, compression, estimtedBucketCount, context);
        }
    }

}