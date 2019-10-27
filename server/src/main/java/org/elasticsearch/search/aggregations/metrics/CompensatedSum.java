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


/**
 * Used to calculate sums using the Kahan summation algorithm.
 *
 * <p>The Kahan summation algorithm (also known as compensated summation) reduces the numerical errors that
 * occur when adding a sequence of finite precision floating point numbers. Numerical errors arise due to
 * truncation and rounding. These errors can lead to numerical instability.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan Summation Algorithm</a>
 */
public class CompensatedSum {

    private static final double NO_CORRECTION = 0.0;

    private final double value;
    private final double delta;

    /**
     * Used to calculate sums using the Kahan summation algorithm.
     *
     * @param value the sum
     * @param delta correction term
     */
    private CompensatedSum(double value, double delta) {
        this.value = value;
        this.delta = delta;
    }

    public static CompensatedSum newInstance(double value, double delta) {
        return new CompensatedSum(value, delta);
    }

    public static CompensatedSum newZeroInstance() {
        return new CompensatedSum(0.0, 0.0);
    }

    /**
     * The value of the sum.
     */
    public double value() {
        return value;
    }

    /**
     * The correction term.
     */
    public double delta() {
        return delta;
    }

    /**
     * Increments the Kahan sum by adding a value and a correction term.
     */
    public CompensatedSum add(double value, double delta) {
        return add(new CompensatedSum(value, delta));
    }

    /**
     * Increments the Kahan sum by adding a value without a correction term.
     */
    public CompensatedSum add(double value) {
        return add(new CompensatedSum(value, NO_CORRECTION));
    }

    /**
     * Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors.
     */
    public CompensatedSum add(CompensatedSum other) {

        if (!Double.isFinite(other.value())) {
            return new CompensatedSum(other.value() + this.value, this.delta);
        }

        if (Double.isFinite(this.value)) {
            double correctedSum = other.value() + (this.delta + other.delta());
            double updatedValue = this.value + correctedSum;
            double updatedDelta = correctedSum - (updatedValue - this.value);
            return new CompensatedSum(updatedValue, updatedDelta);
        }
        return this;
    }

}

