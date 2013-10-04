/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class DeleteWarmerRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteWarmerRequest, DeleteWarmerResponse, DeleteWarmerRequestBuilder> {

    public DeleteWarmerRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new DeleteWarmerRequest());
    }

    public DeleteWarmerRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The name (or wildcard expression) of the index warmer to delete, or null
     * to delete all warmers.
     */
    public DeleteWarmerRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }

    /**
     * Sets the maximum wait for acknowledgement from other nodes
     */
    public DeleteWarmerRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Timeout to wait for the operation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteWarmerRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteWarmerResponse> listener) {
        ((IndicesAdminClient) client).deleteWarmer(request, listener);
    }
}
