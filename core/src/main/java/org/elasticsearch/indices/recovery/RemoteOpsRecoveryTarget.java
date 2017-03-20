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
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class RemoteOpsRecoveryTarget implements OpsRecoveryTargetHandler {

    protected final TransportService transportService;
    protected final long recoveryId;
    protected final ShardId shardId;
    protected final DiscoveryNode targetNode;
    protected final RecoverySettings recoverySettings;

    protected final TransportRequestOptions translogOpsRequestOptions;

    private String targetAllocationId;

    public RemoteOpsRecoveryTarget(long recoveryId, ShardId shardId, String targetAllocationId,
                                   TransportService transportService, DiscoveryNode targetNode,
                                   RecoverySettings recoverySettings) {
        this.targetAllocationId = targetAllocationId;
        this.transportService = transportService;


        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.translogOpsRequestOptions = TransportRequestOptions.builder()
                .withCompress(true)
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout())
                .build();

    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint) {
        transportService.submitRequest(targetNode, PeerRecoveryTargetService.Actions.FINALIZE,
            new RecoveryFinalizeRecoveryRequest(recoveryId, shardId, globalCheckpoint),
            TransportRequestOptions.builder()
                .withTimeout(recoverySettings.internalActionLongTimeout()).build(),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
        final RecoveryTranslogOperationsRequest translogOperationsRequest =
            new RecoveryTranslogOperationsRequest(recoveryId, shardId, operations,
                totalTranslogOps);
        transportService.submitRequest(targetNode,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS, translogOperationsRequest,
                translogOpsRequestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public String getTargetAllocationId() {
        return targetAllocationId;
    }
}
