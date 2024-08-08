/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;
import static org.elasticsearch.core.Strings.format;

public class LocalAllocateDangledIndices {

    private static final Logger logger = LogManager.getLogger(LocalAllocateDangledIndices.class);

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final IndexMetadataVerifier indexMetadataVerifier;

    @Inject
    public LocalAllocateDangledIndices(
        TransportService transportService,
        ClusterService clusterService,
        AllocationService allocationService,
        IndexMetadataVerifier indexMetadataVerifier
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indexMetadataVerifier = indexMetadataVerifier;
        transportService.registerRequestHandler(
            ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            AllocateDangledRequest::new,
            new AllocateDangledRequestHandler()
        );
    }

    public void allocateDangled(Collection<IndexMetadata> indices, ActionListener<AllocateDangledResponse> listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode masterNode = clusterState.nodes().getMasterNode();
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(
            clusterService.localNode(),
            indices.toArray(new IndexMetadata[indices.size()])
        );
        transportService.sendRequest(
            masterNode,
            ACTION_NAME,
            request,
            new ActionListenerResponseHandler<>(listener, AllocateDangledResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
    }

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel, Task task) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].getIndex().getName();
            }
            final String source = "allocation dangled indices " + Arrays.toString(indexNames);

            var listener = new AllocationActionListener<AllocateDangledResponse>(
                new ChannelActionListener<>(channel),
                transportService.getThreadPool().getThreadContext()
            );

            submitUnbatchedTask(source, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.blocks().disableStatePersistence()) {
                        return currentState;
                    }
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(
                        allocationService.getShardRoutingRoleStrategy(),
                        currentState.routingTable()
                    );
                    IndexVersion minIndexCompatibilityVersion = currentState.nodes().getMinSupportedIndexVersion();
                    IndexVersion maxIndexCompatibilityVersion = currentState.nodes().getMaxDataNodeCompatibleIndexVersion();
                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetadata indexMetadata : request.indices) {
                        if (indexMetadata.getCompatibilityVersion().before(minIndexCompatibilityVersion)) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] since it's current compatibility version [{}] "
                                    + "is not supported by at least one node in the cluster minVersion [{}]",
                                indexMetadata.getIndex(),
                                request.fromNode,
                                indexMetadata.getCompatibilityVersion(),
                                minIndexCompatibilityVersion
                            );
                            continue;
                        }
                        if (indexMetadata.getCompatibilityVersion().after(maxIndexCompatibilityVersion)) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] since its current compatibility version [{}] "
                                    + "is later than the maximum supported index version in the cluster [{}]",
                                indexMetadata.getIndex(),
                                request.fromNode,
                                indexMetadata.getCompatibilityVersion(),
                                maxIndexCompatibilityVersion
                            );
                            continue;
                        }
                        if (currentState.metadata().hasIndex(indexMetadata.getIndex().getName())) {
                            continue;
                        }
                        if (currentState.metadata().hasAlias(indexMetadata.getIndex().getName())) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] due to an existing alias with the same name",
                                indexMetadata.getIndex(),
                                request.fromNode
                            );
                            continue;
                        }
                        if (currentState.metadata().indexGraveyard().containsIndex(indexMetadata.getIndex())) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] since it was recently deleted",
                                indexMetadata.getIndex(),
                                request.fromNode
                            );
                            continue;
                        }
                        importNeeded = true;

                        IndexMetadata newIndexMetadata;
                        try {
                            // The dangled index might be from an older version, we need to make sure it's compatible
                            // with the current version.
                            newIndexMetadata = indexMetadataVerifier.verifyIndexMetadata(indexMetadata, minIndexCompatibilityVersion);
                            newIndexMetadata = IndexMetadata.builder(newIndexMetadata)
                                .settings(
                                    Settings.builder()
                                        .put(newIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
                                )
                                .build();
                        } catch (Exception ex) {
                            // upgrade failed - adding index as closed
                            logger.warn(
                                () -> format(
                                    "found dangled index [%s] on node [%s]. This index cannot be "
                                        + "upgraded to the latest version, adding as closed",
                                    indexMetadata.getIndex(),
                                    request.fromNode
                                ),
                                ex
                            );
                            newIndexMetadata = IndexMetadata.builder(indexMetadata)
                                .state(IndexMetadata.State.CLOSE)
                                .version(indexMetadata.getVersion() + 1)
                                .build();
                        }
                        metadata.put(newIndexMetadata, false);
                        blocks.addBlocks(newIndexMetadata);
                        if (newIndexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                            routingTableBuilder.addAsFromDangling(newIndexMetadata);
                        }
                        sb.append("[").append(newIndexMetadata.getIndex()).append("/").append(newIndexMetadata.getState()).append("]");
                    }
                    if (importNeeded == false) {
                        listener.reroute().onResponse(null);
                        return currentState;
                    }
                    logger.info("importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    RoutingTable routingTable = routingTableBuilder.build();
                    ClusterState updatedState = ClusterState.builder(currentState)
                        .metadata(metadata)
                        .blocks(blocks)
                        .routingTable(routingTable)
                        .build();

                    // now, reroute
                    return allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTable).build(),
                        "dangling indices allocated",
                        listener.reroute()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.clusterStateUpdate().onFailure(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    listener.clusterStateUpdate().onResponse(new AllocateDangledResponse());
                }
            });
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    public static class AllocateDangledRequest extends TransportRequest {

        DiscoveryNode fromNode;
        IndexMetadata[] indices;

        public AllocateDangledRequest(StreamInput in) throws IOException {
            super(in);
            fromNode = new DiscoveryNode(in);
            indices = in.readArray(IndexMetadata::readFrom, IndexMetadata[]::new);
        }

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetadata[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fromNode.writeTo(out);
            out.writeArray(indices);
        }
    }

    public static class AllocateDangledResponse extends TransportResponse {

        private AllocateDangledResponse(StreamInput in) throws IOException {
            if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                in.readBoolean();
            }
        }

        private AllocateDangledResponse() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                out.writeBoolean(true);
            }
        }
    }
}
