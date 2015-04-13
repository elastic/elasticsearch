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
 * Needs to be implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that relate to
 * one or more indices. Allows to retrieve which indices the action relates to.
 * In case of internal requests  originated during the distributed execution of an external request,
 * they will still return the indices that the original request related to.
 */
public interface IndicesRequest {

    /**
     * Returns the array of indices that the action relates to
     */
    String[] indices();

    /**
     * Returns the indices options used to resolve indices. They tell for instance whether a single index is
     * accepted, whether an empty array will be converted to _all, and how wildcards will be expanded if needed.
     */
    IndicesOptions indicesOptions();

    static interface Replaceable extends IndicesRequest {
        /*
         * Sets the array of indices that the action relates to
         */
        IndicesRequest indices(String[] indices);
    }
}
