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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

public class RemotePrimaryHandoffRecoveryTarget implements PrimaryHandoffRecoveryTargetHandler {

    private final TransportService transportService;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;


    public RemotePrimaryHandoffRecoveryTarget(long recoveryId, ShardId shardId,
                                              TransportService transportService,
                                              DiscoveryNode targetNode,
                                              RecoverySettings recoverySettings) {
        this.transportService = transportService;


        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;

    }

    @Override
    public void ensureClusterStateVersion(long clusterStateVersion) {
        transportService.submitRequest(targetNode,
            PeerRecoveryTargetService.Actions.WAIT_CLUSTERSTATE,
            new RecoveryWaitForClusterStateRequest(recoveryId, shardId, clusterStateVersion),
            TransportRequestOptions.builder()
                .withTimeout(recoverySettings.internalActionLongTimeout()).build(),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }
}
