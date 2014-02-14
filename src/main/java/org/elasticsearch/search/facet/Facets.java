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

package org.elasticsearch.search.facet;

import java.util.List;
import java.util.Map;

/**
 * Facets of search action.
 *
 *
 */
public interface Facets extends Iterable<Facet> {

    /**
     * The list of {@link Facet}s.
     */
    List<Facet> facets();

    /**
     * Returns the {@link Facet}s keyed by facet name.
     */
    Map<String, Facet> getFacets();

    /**
     * Returns the {@link Facet}s keyed by facet name.
     */
    Map<String, Facet> facetsAsMap();

    /**
     * Returns the facet by name already casted to the specified type.
     */
    <T extends Facet> T facet(Class<T> facetType, String name);

    /**
     * A facet of the specified name.
     */
    <T extends Facet> T facet(String name);
}
