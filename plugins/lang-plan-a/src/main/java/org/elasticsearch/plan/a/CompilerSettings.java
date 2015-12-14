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
 * Settings to use when compiling a script 
 */
final class CompilerSettings {

    private boolean numericOverflow = true;

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
}
