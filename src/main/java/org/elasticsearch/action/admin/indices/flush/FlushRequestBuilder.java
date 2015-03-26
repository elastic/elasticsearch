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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;

/**
 *
 */
public class FlushRequestBuilder extends BroadcastOperationRequestBuilder<FlushRequest, FlushResponse, FlushRequestBuilder, IndicesAdminClient> {

    public FlushRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new FlushRequest());
    }

    public FlushRequestBuilder setForce(boolean force) {
        request.force(force);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<FlushResponse> listener) {
        client.flush(request, listener);
    }

    public FlushRequestBuilder setWaitIfOngoing(boolean waitIfOngoing) {
        request.waitIfOngoing(waitIfOngoing);
        return this;
    }
}
