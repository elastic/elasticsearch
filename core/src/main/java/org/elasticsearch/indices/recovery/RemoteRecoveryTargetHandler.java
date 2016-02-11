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

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {
    private final TransportService transportService;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;

    final TransportRequestOptions recoveryRequestOptions;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService, DiscoveryNode targetNode,
                                       RecoverySettings recoverySettings) {
        this.transportService = transportService;


        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.recoveryRequestOptions = TransportRequestOptions.builder()
                .withCompress(true)
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout())
                .build();
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps) throws IOException {
        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.PREPARE_TRANSLOG,
                new RecoveryPrepareForTranslogOperationsRequest(request.recoveryId(), request.shardId(), totalTranslogOps),
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void finalizeRecovery() {
        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.FINALIZE,
                new RecoveryFinalizeRecoveryRequest(request.recoveryId(), request.shardId()),
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(),
                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void indexTranslogOperations(Iterable<Translog.Operation> operations, int totalTranslogOps) throws RetryTranslogOpsException {
        final RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(
                request.recoveryId(), request.shardId(), operations, totalTranslogOps);
        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                recoveryRequestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps) {

        RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(recoveryId, shardId,
                phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        transportService.submitRequest(targetNode, RecoveryTarget.Actions.FILES_INFO, recoveryInfoFilesRequest,
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();

    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
        transportService.submitRequest(targetNode, RecoveryTarget.Actions.CLEAN_FILES,
                new RecoveryCleanFilesRequest(recoveryId, shardId, sourceMetaData, totalTranslogOps),
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void writeFileChunk(String name, long position, StoreFileMetaData metadata, BytesReference content, long length, boolean
            lastChunk, int totalTranslogOps, long sourceThrottleTimeInNanos, @Nullable RateLimiter rateLimiter) throws IOException {

    }
}
