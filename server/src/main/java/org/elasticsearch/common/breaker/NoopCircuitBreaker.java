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

package org.elasticsearch.common.breaker;

/**
 * A CircuitBreaker that doesn't increment or adjust, and all operations are
 * basically noops
 */
public class NoopCircuitBreaker implements CircuitBreaker {
    public static final int LIMIT = -1;

    private final String name;

    public NoopCircuitBreaker(String name) {
        this.name = name;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        // noop
    }

    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        return 0;
    }

    @Override
    public long addWithoutBreaking(long bytes) {
        return 0;
    }

    @Override
    public long getUsed() {
        return 0;
    }

    @Override
    public long getLimit() {
        return LIMIT;
    }

    @Override
    public double getOverhead() {
        return 0;
    }

    @Override
    public long getTrippedCount() {
        return 0;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Durability getDurability() {
        return Durability.PERMANENT;
    }
}
