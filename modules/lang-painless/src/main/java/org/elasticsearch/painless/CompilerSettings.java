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

package org.elasticsearch.painless;

/**
 * Settings to use when compiling a script.
 */
public final class CompilerSettings {

    /**
     * Constant to be used when specifying numeric overflow when compiling a script.
     */
    public static final String NUMERIC_OVERFLOW = "numeric_overflow";

    /**
     * Constant to be used when specifying the maximum loop counter when compiling a script.
     */
    public static final String MAX_LOOP_COUNTER = "max_loop_counter";

    /**
     * Constant to be used for enabling additional internal compilation checks (slower).
     */
    public static final String PICKY = "picky";

    /**
     * Whether or not to allow numeric values to overflow without exception.
     */
    private boolean numericOverflow = true;

    /**
     * The maximum number of statements allowed to be run in a loop.
     */
    private int maxLoopCounter = 10000;

    /**
     * Whether to throw exception on ambiguity or other internal parsing issues. This option 
     * makes things slower too, it is only for debugging.
     */
    private boolean picky = false;

    /**
     * Returns {@code true} if numeric operations should overflow, {@code false}
     * if they should signal an exception.
     * <p>
     * If this value is {@code true} (default), then things behave like java:
     * overflow for integer types can result in unexpected values / unexpected
     * signs, and overflow for floating point types can result in infinite or
     * {@code NaN} values.
     */
    public final boolean getNumericOverflow() {
        return numericOverflow;
    }

    /**
     * Set {@code true} for numerics to overflow, false to deliver exceptions.
     * @see #getNumericOverflow
     */
    public final void setNumericOverflow(boolean allow) {
        this.numericOverflow = allow;
    }

    /**
     * Returns the value for the cumulative total number of statements that can be made in all loops
     * in a script before an exception is thrown.  This attempts to prevent infinite loops.  Note if
     * the counter is set to 0, no loop counter will be written.
     */
    public final int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /**
     * Set the cumulative total number of statements that can be made in all loops.
     * @see #getMaxLoopCounter
     */
    public final void setMaxLoopCounter(int max) {
        this.maxLoopCounter = max;
    }

    /**
     * Returns true if the compiler should be picky. This means it runs slower and enables additional
     * runtime checks, throwing an exception if there are ambiguities in the grammar or other low level
     * parsing problems.
     */
    public boolean isPicky() {
      return picky;
    }

    /**
     * Set to true if compilation should be picky.
     * @see #isPicky
     */
    public void setPicky(boolean picky) {
      this.picky = picky;
    }
}
