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

package org.elasticsearch.action.bench;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a single iteration of a search benchmark competition
 */
public class CompetitionIteration implements Streamable {

    private long numQueries;
    private SlowRequest[] slowRequests;
    private long totalTime;
    private long sum;
    private long sumTotalHits;
    private double stddev;
    private long min;
    private long max;
    private double mean;
    private double qps;
    private double millisPerHit;
    private double[] percentiles = new double[0];
    private Map<Double, Double> percentileValues = new TreeMap<>();

    private CompetitionIterationData iterationData;

    public CompetitionIteration() { }

    public CompetitionIteration(SlowRequest[] slowestRequests, long totalTime, long numQueries, long sumTotalHits,
                                CompetitionIterationData iterationData) {
        this.totalTime = totalTime;
        this.sumTotalHits = sumTotalHits;
        this.slowRequests = slowestRequests;
        this.numQueries = numQueries;
        this.iterationData = iterationData;
        this.millisPerHit = totalTime / (double)sumTotalHits;
    }

    public void computeStatistics() {

        final SinglePassStatistics single = new SinglePassStatistics();

        for (long datum : iterationData.data()) {
            if (datum > -1) {   // ignore unset values in the underlying array
                single.push(datum);
            }
        }

        sum = single.sum();
        stddev = single.stddev();
        min = single.min();
        max = single.max();
        mean = single.mean();
        qps = numQueries * (1000.d / (double) sum);

        for (double percentile : percentiles) {
            percentileValues.put(percentile, single.percentile(percentile / 100));
        }
    }

    public CompetitionIterationData competitionIterationData() {
        return iterationData;
    }

    public long numQueries() {
        return numQueries;
    }

    public long totalTime() {
        return totalTime;
    }

    public long sumTotalHits() {
        return sumTotalHits;
    }

    public double millisPerHit() {
        return millisPerHit;
    }

    public double queriesPerSecond() {
        return qps;
    }

    public SlowRequest[] slowRequests() {
        return slowRequests;
    }

    public long min() {
        return min;
    }

    public long max() {
        return max;
    }

    public double mean() {
        return mean;
    }

    public Map<Double, Double> percentileValues() {
        return percentileValues;
    }

    public void percentiles(double[] percentiles) {
        this.percentiles = percentiles;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalTime = in.readVLong();
        sumTotalHits = in.readVLong();
        numQueries = in.readVLong();
        millisPerHit = in.readDouble();
        iterationData = in.readOptionalStreamable(new CompetitionIterationData());
        int size = in.readVInt();
        slowRequests = new SlowRequest[size];
        for (int i = 0; i < size; i++) {
            slowRequests[i] = new SlowRequest();
            slowRequests[i].readFrom(in);
        }
        percentiles = in.readDoubleArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalTime);
        out.writeVLong(sumTotalHits);
        out.writeVLong(numQueries);
        out.writeDouble(millisPerHit);
        out.writeOptionalStreamable(iterationData);
        out.writeVInt(slowRequests == null ? 0 : slowRequests.length);
        if (slowRequests != null) {
            for (SlowRequest slowRequest : slowRequests) {
                slowRequest.writeTo(out);
            }
        }
        out.writeDoubleArray(percentiles);
    }

    /**
     * Represents a 'slow' search request
     */
    public static class SlowRequest implements ToXContent, Streamable {

        private long maxTimeTaken;
        private long avgTimeTaken;
        private SearchRequest searchRequest;

        public SlowRequest() { }

        public SlowRequest(long avgTimeTaken, long maxTimeTaken, SearchRequest searchRequest) {
            this.avgTimeTaken = avgTimeTaken;
            this.maxTimeTaken = maxTimeTaken;
            this.searchRequest = searchRequest;
        }

        public long avgTimeTaken() {
            return avgTimeTaken;
        }

        public long maxTimeTaken() {
            return maxTimeTaken;
        }

        public SearchRequest searchRequest() {
            return searchRequest;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            avgTimeTaken = in.readVLong();
            maxTimeTaken = in.readVLong();
            searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(avgTimeTaken);
            out.writeVLong(maxTimeTaken);
            searchRequest.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.MAX_TIME, maxTimeTaken);
            builder.field(Fields.AVG_TIME, avgTimeTaken);
            XContentHelper.writeRawField("request", searchRequest.source(), builder, params);
            return builder;
        }
    }

    static final class Fields {
        static final XContentBuilderString MAX_TIME = new XContentBuilderString("max_time");
        static final XContentBuilderString AVG_TIME = new XContentBuilderString("avg_time");
    }
}
