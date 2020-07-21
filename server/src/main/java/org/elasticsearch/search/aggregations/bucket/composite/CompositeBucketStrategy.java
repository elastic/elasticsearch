/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.Rounding;

/**
 * This class acts as a bit of syntactic sugar to let us pass in the rounding info for dates or the interval for numeric histograms as one
 * class, to save needing three different interfaces.  Sometimes I miss C-style Union structures.
 */
public class CompositeBucketStrategy {
    public enum Strategy {
        ROUNDING,
        INTERVAL,
        NONE
    }

    private final Strategy strategy;

    private final String name;

    // At most one of these should be set
    private final Rounding rounding;
    private final double interval;

    private CompositeBucketStrategy() {
        throw new UnsupportedOperationException();
    }

    public CompositeBucketStrategy(String name) {
        this.strategy = Strategy.NONE;
        this.name = name;
        this.rounding = null;
        this.interval = Double.NaN;
    }

    public CompositeBucketStrategy(String name, Rounding rounding) {
        this.strategy = Strategy.ROUNDING;
        this.name = name;
        this.rounding = rounding;
        this.interval = Double.NaN;
    }

    public CompositeBucketStrategy(String name, double interval) {
        this.strategy = Strategy.INTERVAL;
        this.name = name;
        this.interval = interval;
        this.rounding = null;
    }

    public Rounding getRounding() {
        assert strategy == Strategy.ROUNDING;
        return rounding;
    }

    public double getInterval() {
        assert strategy == Strategy.INTERVAL;
        return interval;
    }

    public String getName() {
        return name;
    }
}
