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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.function.Predicate;

/**
 * Configuration for how {@link Aggregator}s are built.
 */
public class AggregatorImplementation {
    private final Predicate<ValuesSourceType> appliesTo;
    private final AggregatorSupplier aggregatorSupplier;

    public AggregatorImplementation(Predicate<ValuesSourceType> appliesTo, AggregatorSupplier aggregatorSupplier) {
        super();
        this.appliesTo = appliesTo;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    /**
     * Matches {@linkplain ValuesSourceType}s that this implementation supports.
     */
    public Predicate<ValuesSourceType> getAppliesTo() {
        return appliesTo;
    }

    /**
     * The factory for the {@link Aggregator} implementation.
     */
    public AggregatorSupplier getAggregatorSupplier() {
        return aggregatorSupplier;
    }
}