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
package org.elasticsearch.cluster.ack;

/**
 * Base cluster state update request that allows to execute update against multiple indices
 */
public abstract class IndicesClusterStateUpdateRequest<T extends IndicesClusterStateUpdateRequest<T>> extends ClusterStateUpdateRequest<T> {

    private String[] indices;

    /**
     * Returns the indices the operation needs to be executed on
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices the operation needs to be executed on
     */
    @SuppressWarnings("unchecked")
    public T indices(String[] indices) {
        this.indices = indices;
        return (T)this;
    }
}
