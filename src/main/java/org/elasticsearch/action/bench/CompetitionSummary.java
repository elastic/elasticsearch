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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import org.apache.lucene.util.CollectionUtil;

import java.io.IOException;
import java.util.*;

/**
 * Summary statistics for a benchmark search competition.
 *
 * Statistics are calculated over all iteration results for all nodes
 * that executed the competition.
 *
 * Since values are calculated lazily on first access, users of this class
 * should first call computeSummaryStatistics() prior to accessing individual
 * measurements.
 */
public class CompetitionSummary implements ToXContent {

    private List<CompetitionNodeResult> nodeResults;
    private boolean computed = false;

    private long min = 0;
    private long max = 0;
    private long totalTime = 0;
    private long sumTotalHits = 0;
    private long totalIterations = 0;
    private long completedIterations = 0;
    private long totalQueries = 0;
    private double avgWarmupTime = 0;
    private int concurrency = 0;
    private int multiplier = 0;
    private double mean = 0;
    private double millisPerHit = 0.0;
    private double stdDeviation = 0.0;
    private double queriesPerSecond = 0.0;
    private double[] percentiles;
    Map<Double, Double> percentileValues = new TreeMap<>();

    private List<Tuple<String, CompetitionIteration.SlowRequest>> slowest = new ArrayList<>();

    public CompetitionSummary() { }

    public CompetitionSummary(List<CompetitionNodeResult> nodeResults, int concurrency, int multiplier, double[] percentiles) {
        this.nodeResults = nodeResults;
        this.concurrency = concurrency;
        this.multiplier = multiplier;
        this.percentiles = percentiles;
    }

    /**
     * Gets node-level competition results
     * @return  A list of node-level competition results
     */
    public List<CompetitionNodeResult> nodeResults() {
        return nodeResults;
    }

    /**
     * Calculates statistical measures from raw measurements. Should be called prior to accessing
     * individual measurements.
     */
    public void computeSummaryStatistics() {

        if (computed) {
            return;
        }

        long totalWarmupTime = 0;
        final SinglePassStatistics single = new SinglePassStatistics();

        for (CompetitionNodeResult nodeResult : nodeResults) {

            totalWarmupTime += nodeResult.warmUpTime();
            totalIterations += nodeResult.totalIterations();
            completedIterations += nodeResult.completedIterations();

            // only calculate statistics for iterations completed thus far
            for (int i = 0; i < nodeResult.completedIterations(); i++) {

                CompetitionIteration competitionIteration = nodeResult.iterations().get(i);
                CompetitionIterationData iterationData = competitionIteration.competitionIterationData();
                long[] data = iterationData.data();

                for (long datum : data) {
                    if (datum > -1) {   // ignore unset values in the underlying array
                        single.push(datum);
                    }
                }

                totalQueries += competitionIteration.numQueries();
                totalTime += competitionIteration.totalTime();
                sumTotalHits += competitionIteration.sumTotalHits();

                // keep track of slowest requests
                if (competitionIteration.slowRequests() != null) {
                    for (CompetitionIteration.SlowRequest slow : competitionIteration.slowRequests()) {
                        slowest.add(new Tuple<>(nodeResult.nodeName(), slow));
                    }
                }
            }
        }

        min = single.min();
        max = single.max();
        mean = single.mean();
        stdDeviation = single.stddev();
        avgWarmupTime = (nodeResults.size() > 0) ? totalWarmupTime / nodeResults.size() : 0.0;
        queriesPerSecond = (single.sum() > 0) ? (totalQueries * (1000.0 / (double) single.sum())) : 0.0;
        millisPerHit = (sumTotalHits > 0) ? (totalTime / (double) sumTotalHits) : 0.0;

        for (double percentile : percentiles) {
            percentileValues.put(percentile, single.percentile(percentile / 100.0d));
        }

        CollectionUtil.timSort(slowest, new Comparator<Tuple<String, CompetitionIteration.SlowRequest>>() {
            @Override
            public int compare(Tuple<String, CompetitionIteration.SlowRequest> o1, Tuple<String, CompetitionIteration.SlowRequest> o2) {
                return Long.compare(o2.v2().maxTimeTaken(), o1.v2().maxTimeTaken());
            }
        });
        computed = true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        computeSummaryStatistics();

        builder.startObject(Fields.SUMMARY);
        builder.startArray(Fields.NODES);
        for (CompetitionNodeResult nodeResult : nodeResults) {
            builder.field(nodeResult.nodeName());
        }
        builder.endArray();

        builder.field(Fields.TOTAL_ITERATIONS, totalIterations);
        builder.field(Fields.COMPLETED_ITERATIONS, completedIterations);
        builder.field(Fields.TOTAL_QUERIES, totalQueries);
        builder.field(Fields.CONCURRENCY, concurrency);
        builder.field(Fields.MULTIPLIER, multiplier);
        builder.field(Fields.AVG_WARMUP_TIME, avgWarmupTime);

        builder.startObject(Fields.STATISTICS);
        builder.field(Fields.MIN, min == Long.MAX_VALUE ? 0 : min);
        builder.field(Fields.MAX, max == Long.MIN_VALUE ? 0 : max);
        builder.field(Fields.MEAN, mean);
        builder.field(Fields.QPS, queriesPerSecond);
        builder.field(Fields.STD_DEV, stdDeviation);
        builder.field(Fields.MILLIS_PER_HIT, millisPerHit);

        for (Map.Entry<Double, Double> entry : percentileValues.entrySet()) {
            // Change back to integral value for display purposes
            builder.field(new XContentBuilderString("percentile_" + entry.getKey().longValue()),
                    (entry.getValue().isNaN()) ? 0.0 : entry.getValue());
        }

        builder.endObject();

        if (totalIterations > 0 && slowest.size() > 0) {
            builder.startArray(Fields.SLOWEST);
            int n = (int) (slowest.size() / totalIterations);
            for (int i = 0; i < n; i++) {
                builder.startObject();
                builder.field(Fields.NODE, slowest.get(i).v1());
                slowest.get(i).v2().toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    /**
     * List of per-node competition results
     * @return  Per-node competition results
     */
    public List<CompetitionNodeResult> getNodeResults() {
        return nodeResults;
    }

    /**
     * Shortest execution time of any search
     * @return  Shortest execution time of any search
     */
    public long getMin() {
        return min;
    }

    /**
     * Longest execution time of any search
     * @return  Longest execution time of any search
     */
    public long getMax() {
        return max;
    }

    /**
     * Total execution time
     * @return  Total execution time
     */
    public long getTotalTime() {
        return totalTime;
    }

    /**
     * Total hit count
     * @return  Total hit count
     */
    public long getSumTotalHits() {
        return sumTotalHits;
    }

    /**
     * Number of requested iterations
     * @return  Number of requested iterations
     */
    public long getTotalIterations() {
        return totalIterations;
    }

    /**
     * Number of iterations actually completed
     * @return  Number of iterations actually completed
     */
    public long getCompletedIterations() {
        return completedIterations;
    }

    /**
     * Total number of queries actually executed
     * @return  Number of queries actually executed
     */
    public long getTotalQueries() {
        return totalQueries;
    }

    /**
     * Mean average of warmup times across all nodes
     * @return  Average warmup time
     */
    public double getAvgWarmupTime() {
        return avgWarmupTime;
    }

    /**
     * Number of concurrent searches
     * @return  Concurrency
     */
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * Loop multiplier
     * @return  Multiplier
     */
    public int getMultiplier() {
        return multiplier;
    }

    /**
     * Mean average
     * @return  Mean average
     */
    public double getMean() {
        return mean;
    }

    /**
     * Total time considered as a percentage of total hits
     * @return  Milliseconds-per-hit
     */
    public double getMillisPerHit() {
        return millisPerHit;
    }

    /**
     * Standard deviation from the mean of all measurements
     * @return  Standard deviation
     */
    public double getStdDeviation() {
        return stdDeviation;
    }

    /**
     * Measurement of the queries-per-second calculated as: numQueries * (1000.0 / totalTime)
     * @return  Queries-per-second
     */
    public double getQueriesPerSecond() {
        return queriesPerSecond;
    }

    /**
     * The user-requested percentiles to measure
     * @return  Array of percentiles to measure
     */
    public double[] getPercentiles() {
        return percentiles;
    }

    /**
     * A map of percentiles and their measurements.
     * @return  A map of entries of (percentile, measurement)
     */
    public Map<Double, Double> getPercentileValues() {
        return percentileValues;
    }

    /**
     * A list of the N slowest requests and the node that each executed on.
     * @return  A list of pairs of (node, request)
     */
    public List<Tuple<String, CompetitionIteration.SlowRequest>> getSlowest() {
        return slowest;
    }

    static final class Fields {
        static final XContentBuilderString SUMMARY = new XContentBuilderString("summary");
        static final XContentBuilderString NODES = new XContentBuilderString("nodes");
        static final XContentBuilderString TOTAL_ITERATIONS = new XContentBuilderString("total_iterations");
        static final XContentBuilderString COMPLETED_ITERATIONS = new XContentBuilderString("completed_iterations");
        static final XContentBuilderString TOTAL_QUERIES = new XContentBuilderString("total_queries");
        static final XContentBuilderString CONCURRENCY = new XContentBuilderString("concurrency");
        static final XContentBuilderString MULTIPLIER = new XContentBuilderString("multiplier");
        static final XContentBuilderString AVG_WARMUP_TIME = new XContentBuilderString("avg_warmup_time");
        static final XContentBuilderString STATISTICS = new XContentBuilderString("statistics");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString QPS = new XContentBuilderString("qps");
        static final XContentBuilderString STD_DEV = new XContentBuilderString("std_dev");
        static final XContentBuilderString MILLIS_PER_HIT = new XContentBuilderString("millis_per_hit");
        static final XContentBuilderString SLOWEST = new XContentBuilderString("slowest");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
    }
}

