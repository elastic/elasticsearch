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
package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;

/**
 * Generic interface to group ActionRequest, which work on single document level
 *
 * Forces this class return index/type/id getters
 */
public interface DocumentRequest<T> {

    /**
     * Get the index that this request operates on
     * @return the index
     */
    String index();

    /**
     * Get the type that this request operates on
     * @return the type
     */
    String type();

    /**
     * Get the id of the document for this request
     * @return the id
     */
    String id();

    /**
     * Get the options for this request
     * @return the indices options
     */
    IndicesOptions indicesOptions();

    /**
     * Set the routing for this request
     * @param routing
     * @return the Request
     */
    T routing(String routing);

    /**
     * Get the routing for this request
     * @return the Routing
     */
    String routing();
}
