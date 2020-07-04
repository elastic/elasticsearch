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

package org.elasticsearch.search.aggregations.metrics;

public class M2Calculator {

    private static final double NO_CORRECTION = 0.0;

    private long count;
    private double m2;
    private double mean;

    /**
     * Used to calculate sums vairance using Welford's online algorithm.
     * M2 aggregates the squared distance from the mean
     *
     *
     * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">
     *         Welford's_online_algorithm</a>
     */
    public M2Calculator(double m2, long count, double sum) {
        this.m2 = m2;
        this.count = count;
        this.mean = count > 0 ? sum / count : 0;
    }


    public double value() {
        return m2;
    }

    /**
     * Resets the internal state to use the new value
     */
    public void reset(double m2, long count, double sum) {
        this.m2 = m2;
        this.count = count;
        this.mean = count > 0 ? sum / count : 0;
    }

    public M2Calculator add(double newValue) {
        if (Double.isFinite(newValue) == false) {
            this.m2 = newValue + this.m2;
        } else {
            this.count = count + 1;
            double delta = newValue - this.mean;
            this.mean += delta / count;
            double delta2 = newValue - this.mean;
            this.m2 += delta * delta2;
        }
        return this;

    }

}

