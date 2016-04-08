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

import java.util.List;
import java.util.Map;

/**
 * Represents a set of computed addAggregation.
 */
public interface Aggregations extends Iterable<Aggregation> {

    /**
     * The list of {@link Aggregation}s.
     */
    List<Aggregation> asList();

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    Map<String, Aggregation> asMap();

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    Map<String, Aggregation> getAsMap();

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    <A extends Aggregation> A get(String name);

    /**
     * Get the value of specified path in the aggregation.
     * 
     * @param path
     *            the path to the property in the aggregation tree
     * @return the value of the property
     */
    Object getProperty(String path);

}
