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
package org.elasticsearch.action.percolate;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A builder for to ease the use of defining a {@link MultiPercolateRequest} instance.
 */
public class MultiPercolateRequestBuilder extends ActionRequestBuilder<MultiPercolateRequest, MultiPercolateResponse, MultiPercolateRequestBuilder> {

    public MultiPercolateRequestBuilder(ElasticsearchClient client, MultiPercolateAction action) {
        super(client, action, new MultiPercolateRequest());
    }

    /**
     * Bundles the specified percolate request to the multi percolate request.
     */
    public MultiPercolateRequestBuilder add(PercolateRequest percolateRequest) {
        request.add(percolateRequest);
        return this;
    }

    /**
     * Bundles the specified percolate request build to the multi percolate request.
     */
    public MultiPercolateRequestBuilder add(PercolateRequestBuilder percolateRequestBuilder) {
        request.add(percolateRequestBuilder);
        return this;
    }

    /**
     * Specifies how to globally ignore indices that are not available and how to deal with wildcard indices expressions.
     * <p/>
     * Invoke this method before invoking {@link #add(PercolateRequestBuilder)}.
     */
    public MultiPercolateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
