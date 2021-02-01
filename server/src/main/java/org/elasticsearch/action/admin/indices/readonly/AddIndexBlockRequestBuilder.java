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

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;

/**
 * Builder for add index block request
 */
public class AddIndexBlockRequestBuilder
    extends AcknowledgedRequestBuilder<AddIndexBlockRequest, AddIndexBlockResponse, AddIndexBlockRequestBuilder> {

    public AddIndexBlockRequestBuilder(ElasticsearchClient client, AddIndexBlockAction action, APIBlock block, String... indices) {
        super(client, action, new AddIndexBlockRequest(block, indices));
    }

    /**
     * Sets the indices to be blocked
     *
     * @param indices the indices to be blocked
     * @return the request itself
     */
    public AddIndexBlockRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and indices wildcard expressions
     * @return the request itself
     */
    public AddIndexBlockRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
