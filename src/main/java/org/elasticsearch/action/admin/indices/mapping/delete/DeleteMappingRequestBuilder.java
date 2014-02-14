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

package org.elasticsearch.action.admin.indices.mapping.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 * Builder for a delete mapping request
 */
public class DeleteMappingRequestBuilder extends AcknowledgedRequestBuilder<DeleteMappingRequest, DeleteMappingResponse, DeleteMappingRequestBuilder> {

    public DeleteMappingRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new DeleteMappingRequest());
    }

    /**
     * Sets the indices the delete mapping will execute on
     */
    public DeleteMappingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Sets the type of the mapping to remove
     */
    public DeleteMappingRequestBuilder setType(String... types) {
        request.types(types);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
     * For example indices that don't exist.
     */
    public DeleteMappingRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteMappingResponse> listener) {
        ((IndicesAdminClient) client).deleteMapping(request, listener);
    }
}
