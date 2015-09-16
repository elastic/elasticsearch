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

package org.elasticsearch.action.admin.cluster.node.liveness;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

public final class TransportLivenessAction implements TransportRequestHandler<LivenessRequest> {

    private final ClusterService clusterService;
    private final ClusterName clusterName;
    public static final String NAME = "cluster:monitor/nodes/liveness";

    @Inject
    public TransportLivenessAction(ClusterName clusterName,
                                   ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.clusterName = clusterName;
        transportService.registerRequestHandler(NAME, LivenessRequest::new, ThreadPool.Names.SAME, this);
    }

    @Override
    public void messageReceived(LivenessRequest request, TransportChannel channel) throws Exception {
        channel.sendResponse(new LivenessResponse(clusterName, clusterService.localNode()));
    }
}
