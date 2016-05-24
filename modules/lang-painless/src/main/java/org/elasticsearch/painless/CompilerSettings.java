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
     * Constant to be used when specifying the maximum loop counter when compiling a script.
     */
    public static final String MAX_LOOP_COUNTER = "max_loop_counter";

    /**
     * Constant to be used for enabling additional internal compilation checks (slower).
     */
    public static final String PICKY = "picky";

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
