/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class DeleteIndexTemplateRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteIndexTemplateRequest, DeleteIndexTemplateResponse, DeleteIndexTemplateRequestBuilder> {

    public DeleteIndexTemplateRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new DeleteIndexTemplateRequest());
    }

    public DeleteIndexTemplateRequestBuilder(IndicesAdminClient indicesClient, String name) {
        super((InternalIndicesAdminClient) indicesClient, new DeleteIndexTemplateRequest(name));
    }

    /**
     * Timeout to wait for the index deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteIndexTemplateRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Timeout to wait for the index deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteIndexTemplateRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteIndexTemplateResponse> listener) {
        ((IndicesAdminClient) client).deleteTemplate(request, listener);
    }
}
