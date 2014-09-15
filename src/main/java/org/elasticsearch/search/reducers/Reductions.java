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
package org.elasticsearch.search.reducers;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;
import java.util.Map;

/**
 * Represents a set of computed addAggregation.
 */
public interface Reductions extends Iterable<Reduction> {

    /**
     * The list of {@link Aggregation}s.
     */
    List<Reduction> asList();

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    Map<String, Reduction> asMap();

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    Map<String, Reduction> getAsMap();

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    <A extends Reduction> A get(String name);

}
