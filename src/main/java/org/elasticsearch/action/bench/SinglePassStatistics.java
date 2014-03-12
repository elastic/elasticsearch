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

import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;

/**
 * Utility class for accurately measuring statistical variance in cases where
 * it is not possible or sensible to retain an entire data set in memory.
 *
 * Mean and variance algorithms taken from Donald Knuth's Art of Computer Programming, Vol 2, page 232, 3rd edition.
 * Based on reference implementation http://www.johndcook.com/standard_deviation.html.
 *
 * For each input, the running mean and running sum-of-squares variance is calculated according
 * to:
 *      Running mean:     Mk = Mk-1+ (xk - Mk-1)/k
 *      Running variance: Sk = Sk-1 + (xk - Mk-1)*(xk - Mk)
 *
 * Percentile computations use T-Digest: https://github.com/tdunning/t-digest
 */
public class SinglePassStatistics {

    private long count = 0;
    private double runningMean = 0.0;
    private double runningVariance = 0.0;
    private long runningSum = 0;

    TDigestState tdigest = new TDigestState(100.0);

    /**
     * Adds a new value onto the running calculation
     *
     * @param value     New value to add to calculation
     */
    public void push(long value) {
        count++;
        if (count == 1) {
            runningMean = value;
            runningVariance = 0.0;
        } else {
            double newMean = runningMean + (( value - runningMean) / count );                       // Mk = Mk-1 + ((xk - Mk-1) / k)
            double newVariance = runningVariance + ( (value - runningMean) * (value - newMean) );   // Sk = Sk-1 + (xk - Mk-1)*(xk - Mk)

            runningMean = newMean;
            runningVariance = newVariance;
        }

        runningSum += value;
        tdigest.add(value);
    }

    /**
     * Current value for the running mean
     *
     * @return      Current value of running mean
     */
    public double mean() {
        return runningMean;
    }

    /**
     * Current value for the running variance from the mean
     *
     * @return      Current value for the running variance
     */
    public double variance() {
        if (count > 1) {
            return runningVariance / (count - 1);
        } else {
            return 0.0;
        }
    }

    /**
     * Current running value of standard deviation
     *
     * @return      Current running value of standard deviation
     */
    public double stddev() {
        return Math.sqrt(variance());
    }

    /**
     * Minimum value in data set
     *
     * @return      Minimum value in data set
     */
    public long min() {
        return (long) tdigest.quantile(0.0);
    }

    /**
     * Maximum value in data set
     * @return      Maximum value in data set
     */
    public long max() {
        return (long) tdigest.quantile(1.0);
    }

    /**
     * Total number of values seen
     *
     * @return      Total number of values seen
     */
    public long count() {
        return count;
    }

    /**
     * Running sum of all values
     *
     * @return      Running sum of all values
     */
    public long sum() {
        return runningSum;
    }

    /**
     * Running percentile
     * @param q     Percentile to calculate
     * @return      Running percentile
     */
    public double percentile(double q) {
        return tdigest.quantile(q);
    }
}
