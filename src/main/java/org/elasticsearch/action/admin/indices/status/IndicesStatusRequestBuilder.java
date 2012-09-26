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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 *
 */
public class IndicesStatusRequestBuilder extends BroadcastOperationRequestBuilder<IndicesStatusRequest, IndicesStatusResponse, IndicesStatusRequestBuilder> {

    public IndicesStatusRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new IndicesStatusRequest());
    }

    /**
     * Should the status include recovery information. Defaults to <tt>false</tt>.
     */
    public IndicesStatusRequestBuilder setRecovery(boolean recovery) {
        request.recovery(recovery);
        return this;
    }

    /**
     * Should the status include recovery information. Defaults to <tt>false</tt>.
     */
    public IndicesStatusRequestBuilder setSnapshot(boolean snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<IndicesStatusResponse> listener) {
        ((IndicesAdminClient) client).status(request, listener);
    }
}
