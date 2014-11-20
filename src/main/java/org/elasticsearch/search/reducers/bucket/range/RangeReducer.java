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

package org.elasticsearch.search.reducers.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RangeReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalRange.TYPE.stream());
    }

    private RangeAggregator.Range[] ranges;
    private String fieldName;
    private double[] maxTo;
    private String bucketsPath;

    public RangeReducer(String name, String bucketsPath, String fieldName, RangeAggregator.Range[] ranges, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, factories, context, parent);
        this.bucketsPath = bucketsPath;
        this.ranges = ranges;
        this.fieldName = fieldName;

        RangeAggregator.sortRanges(this.ranges);

        maxTo = new double[this.ranges.length];
        maxTo[0] = this.ranges[0].to;
        for (int i = 1; i < this.ranges.length; ++i) {
            maxTo[i] = Math.max(this.ranges[i].to,maxTo[i-1]);
        }
    }

    @Override
    public InternalBucketReducerAggregation reduce(Aggregations aggregationsTree, Aggregation currentAggregation) {
        MultiBucketsAggregation aggregation;
        if (currentAggregation == null) {
            Object o = aggregationsTree.getProperty(bucketsPath);
            if (o instanceof MultiBucketsAggregation) {
               aggregation = (MultiBucketsAggregation) o;
            } else {
                throw new ReductionExecutionException("bucketsPath must resolve to a muti-bucket aggregation for reducer [" + name() + "]");
            }
        } else {
            if (currentAggregation instanceof MultiBucketsAggregation) {
                aggregation = (MultiBucketsAggregation) currentAggregation;
            } else {
                throw new ReductionExecutionException("aggregation must be a muti-bucket aggregation for reducer [" + name() + "]");
            }
        }
        return doReduce(aggregationsTree, aggregation);
    }

    private InternalBucketReducerAggregation doReduce(Aggregations aggregationsTree, MultiBucketsAggregation aggregation) throws ReductionExecutionException {
        List<InternalSelection> selections = new ArrayList<>(ranges.length);
        List<? extends Bucket> aggBuckets = aggregation.getBuckets();

        List<List<Bucket>> rangeBuckets = new ArrayList<>();
        for (RangeAggregator.Range range : ranges) {
            rangeBuckets.add(new ArrayList<Bucket>());
        }

        List<String> path = new ArrayList<>(1);
        path.add(fieldName);

        for (Bucket bucket : aggBuckets) {
            double value = (double) bucket.getProperty(fieldName, path);
            addToRanges(value, bucket, rangeBuckets);
        }

        for (int i = 0; i < rangeBuckets.size(); i++) {
            List<Bucket> selectionBuckets  = rangeBuckets.get(i);

            String selectionKey;
            if (selectionBuckets.size() == 0) {
                continue;
            } else {
                selectionKey = ranges[i].toString();
            }
            InternalSelection selection = new InternalSelection(selectionKey, ((InternalAggregation) aggregation).type().stream(),
                    selectionBuckets, InternalAggregations.EMPTY);
            InternalAggregations subReducersResults = runSubReducers(aggregationsTree, selection);
            selection.setAggregations(subReducersResults);
            selections.add(selection);
        }

        return new InternalRange(name(), selections);
    }

    private void addToRanges(double value, Bucket bucket, List<List<Bucket>> rangeBuckets) {

        int lo = 0, hi = ranges.length - 1; // all candidates are between these indexes
        int mid = (lo + hi) >>> 1;
        while (lo <= hi) {
            if (value < ranges[mid].from) {
                hi = mid - 1;
            } else if (value >= maxTo[mid]) {
                lo = mid + 1;
            } else {
                break;
            }
            mid = (lo + hi) >>> 1;
        }
        if (lo > hi) return; // lo; // no potential candidate

        // binary search the lower bound
        int startLo = lo, startHi = mid;
        while (startLo <= startHi) {
            final int startMid = (startLo + startHi) >>> 1;
            if (value >= maxTo[startMid]) {
                startLo = startMid + 1;
            } else {
                startHi = startMid - 1;
            }
        }

        // binary search the upper bound
        int endLo = mid, endHi = hi;
        while (endLo <= endHi) {
            final int endMid = (endLo + endHi) >>> 1;
            if (value < ranges[endMid].from) {
                endHi = endMid - 1;
            } else {
                endLo = endMid + 1;
            }
        }

        assert startLo == 0 || value >= maxTo[startLo - 1];
        assert endHi == ranges.length - 1 || value < ranges[endHi + 1].from;

        for (int i = startLo; i <= endHi; ++i) {
            if (ranges[i].matches(value)) {
                //collectBucket(doc, subBucketOrdinal(owningBucketOrdinal, i));
                rangeBuckets.get(i).add(bucket);
            }
        }
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        private RangeAggregator.Range[] ranges;
        private String fieldName;

        public Factory() {
            super(InternalRange.TYPE);
        }

        public Factory(String name, String bucketsPath, String fieldName, ArrayList<RangeAggregator.Range> ranges) {
            super(name, InternalRange.TYPE);
            this.bucketsPath = bucketsPath;
            this.ranges = ranges.toArray(new RangeAggregator.Range[ranges.size()]);;
            this.fieldName = fieldName;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new RangeReducer(name, bucketsPath, fieldName, ranges, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            fieldName = in.readString();
            //NOCOMMIT ranges here
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeString(fieldName);

        }
        
    }

}
