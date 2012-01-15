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
import org.elasticsearch.action.admin.indices.support.BaseIndicesRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class DeleteIndexTemplateRequestBuilder extends BaseIndicesRequestBuilder<DeleteIndexTemplateRequest, DeleteIndexTemplateResponse> {

    public DeleteIndexTemplateRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new DeleteIndexTemplateRequest());
    }

    public DeleteIndexTemplateRequestBuilder(IndicesAdminClient indicesClient, String name) {
        super(indicesClient, new DeleteIndexTemplateRequest(name));
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

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    public DeleteIndexTemplateRequestBuilder setMasterNodeTimeout(TimeValue timeout) {
        request.masterNodeTimeout(timeout);
        return this;
    }

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    public DeleteIndexTemplateRequestBuilder setMasterNodeTimeout(String timeout) {
        request.masterNodeTimeout(timeout);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteIndexTemplateResponse> listener) {
        client.deleteTemplate(request, listener);
    }
}
