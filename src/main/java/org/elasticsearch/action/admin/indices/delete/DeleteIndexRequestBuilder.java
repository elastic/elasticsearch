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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class DeleteIndexRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteIndexRequest, DeleteIndexResponse, DeleteIndexRequestBuilder, IndicesAdminClient> {

    public DeleteIndexRequestBuilder(IndicesAdminClient indicesClient, String... indices) {
        super(indicesClient, new DeleteIndexRequest(indices));
    }

    /**
     * Timeout to wait for the index deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>60s</tt>.
     */
    public DeleteIndexRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Timeout to wait for the index deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteIndexRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
     * For example indices that don't exist.
     */
    public DeleteIndexRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteIndexResponse> listener) {
        client.delete(request, listener);
    }
}
