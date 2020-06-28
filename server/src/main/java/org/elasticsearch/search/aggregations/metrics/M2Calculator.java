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

    private double value;
    private long count;
    private double mean;

    /**
     * Used to calculate sums using the Welford's online algorithm.
     *
     * @param m2 the M2
     */
    public M2Calculator(double m2, long count, double compensatedSum) {
        this.value = m2;
        this.count = count;
        this.mean = count > 0 ? compensatedSum/count : 0;
    }

    /**
     * M2 value.
     */
    public double value() {
        return value;
    }

    /**
     * Resets the internal state to use the new value and compensation delta
     */
    public void reset(double value, long count, double compensatedSum) {
        this.value = value;
        this.count = count;
        this.mean = count > 0 ? compensatedSum/count : 0;
    }

    public M2Calculator add(double value) {
        if (Double.isFinite(value) == false) {
            this.value = value + this.value;
        }

        else {
            this.count = count + 1;
            double delta = value - this.mean;
            this.mean += delta/count;
            double delta2 = value - this.mean;
            this.value += delta * delta2;
        }
        return this;

    }


}

