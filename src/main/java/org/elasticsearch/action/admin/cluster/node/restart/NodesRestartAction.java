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

package org.elasticsearch.action.admin.cluster.node.restart;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

/**
 */
public class NodesRestartAction extends ClusterAction<NodesRestartRequest, NodesRestartResponse, NodesRestartRequestBuilder> {

    public static final NodesRestartAction INSTANCE = new NodesRestartAction();
    public static final String NAME = "cluster/nodes/restart";

    private NodesRestartAction() {
        super(NAME);
    }

    @Override
    public NodesRestartResponse newResponse() {
        return new NodesRestartResponse();
    }

    @Override
    public NodesRestartRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new NodesRestartRequestBuilder(client);
    }
}
