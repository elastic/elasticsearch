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

package org.elasticsearch.plan.a;

/**
 * Settings to use when compiling a script.
 */
final class CompilerSettings {
    /**
     * Constant to be used when specifying numeric overflow when compiling a script.
     */
    public static final String NUMERIC_OVERFLOW = "numeric_overflow";

    /**
     * Constant to be used when specifying the maximum loop counter when compiling a script.
     */
    public static final String MAX_LOOP_COUNTER = "max_loop_counter";

    /**
     * Whether or not to allow numeric values to overflow without exception.
     */
    private boolean numericOverflow = true;

    /**
     * The maximum number of statements allowed to be run in a loop.
     */
    private int maxLoopCounter = 10000;

    /**
     * Returns {@code true} if numeric operations should overflow, {@code false}
     * if they should signal an exception.
     * <p>
     * If this value is {@code true} (default), then things behave like java:
     * overflow for integer types can result in unexpected values / unexpected
     * signs, and overflow for floating point types can result in infinite or
     * {@code NaN} values.
     */
    public boolean getNumericOverflow() {
        return numericOverflow;
    }

    /**
     * Set {@code true} for numerics to overflow, false to deliver exceptions.
     * @see #getNumericOverflow
     */
    public void setNumericOverflow(boolean allow) {
        this.numericOverflow = allow;
    }

    /**
     * Returns the value for the cumulative total number of statements that can be made in all loops
     * in a script before an exception is thrown.  This attempts to prevent infinite loops.
     */
    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /**
     * Set the cumulative total number of statements that can be made in all loops.
     * @see #getMaxLoopCounter
     */
    public void setMaxLoopCounter(int max) {
        this.maxLoopCounter = max;
    }
}
