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

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Settings for a {@link CircuitBreaker}
 */
public class BreakerSettings {

    private final CircuitBreaker.Name name;
    private final long limitBytes;
    private final double overhead;

    public BreakerSettings(CircuitBreaker.Name name, long limitBytes, double overhead) {
        this.name = name;
        this.limitBytes = limitBytes;
        this.overhead = overhead;
    }

    public CircuitBreaker.Name getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limitBytes;
    }

    public double getOverhead() {
        return this.overhead;
    }

    @Override
    public String toString() {
        return "[" + this.name.toString() +
                ",limit=" + this.limitBytes + "/" + new ByteSizeValue(this.limitBytes) +
                ",overhead=" + this.overhead + "]";
    }
}
