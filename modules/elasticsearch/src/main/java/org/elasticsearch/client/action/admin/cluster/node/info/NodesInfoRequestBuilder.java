/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.client.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.action.admin.cluster.support.BaseClusterRequestBuilder;

/**
 * @author kimchy (shay.banon)
 */
public class NodesInfoRequestBuilder extends BaseClusterRequestBuilder<NodesInfoRequest, NodesInfoResponse> {

    public NodesInfoRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new NodesInfoRequest());
    }

    public NodesInfoRequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return this;
    }

    @Override protected void doExecute(ActionListener<NodesInfoResponse> listener) {
        client.nodesInfo(request, listener);
    }
}
