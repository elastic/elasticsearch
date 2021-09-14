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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

public class LocalAllocateDangledIndices {

    private static final Logger logger = LogManager.getLogger(LocalAllocateDangledIndices.class);

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexMetadataVerifier indexMetadataVerifier;

    @Inject
    public LocalAllocateDangledIndices(TransportService transportService, ClusterService clusterService,
                                       AllocationService allocationService, IndexMetadataVerifier indexMetadataVerifier) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indexMetadataVerifier = indexMetadataVerifier;
        transportService.registerRequestHandler(ACTION_NAME, ThreadPool.Names.SAME, AllocateDangledRequest::new,
            new AllocateDangledRequestHandler());
    }

    public void allocateDangled(Collection<IndexMetadata> indices, ActionListener<AllocateDangledResponse> listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode masterNode = clusterState.nodes().getMasterNode();
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(clusterService.localNode(),
            indices.toArray(new IndexMetadata[indices.size()]));
        transportService.sendRequest(masterNode, ACTION_NAME, request,
            new ActionListenerResponseHandler<>(listener, AllocateDangledResponse::new, ThreadPool.Names.SAME));
    }

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel, Task task) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].getIndex().getName();
            }
            clusterService.submitStateUpdateTask("allocation dangled indices " + Arrays.toString(indexNames), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.blocks().disableStatePersistence()) {
                        return currentState;
                    }
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                        .minimumIndexCompatibilityVersion();
                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetadata indexMetadata : request.indices) {
                        if (indexMetadata.getCreationVersion().before(minIndexCompatibilityVersion)) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] since it's created version [{}] is not supported by at "
                                    + "least one node in the cluster minVersion [{}]",
                                indexMetadata.getIndex(),
                                request.fromNode,
                                indexMetadata.getCreationVersion(),
                                minIndexCompatibilityVersion
                            );
                            continue;
                        }
                        if (currentState.nodes().getMinNodeVersion().before(indexMetadata.getCreationVersion())) {
                            logger.warn("ignoring dangled index [{}] on node [{}]" +
                                " since its created version [{}] is later than the oldest versioned node in the cluster [{}]",
                                indexMetadata.getIndex(), request.fromNode, indexMetadata.getCreationVersion(),
                                currentState.getNodes().getMasterNode().getVersion());
                            continue;
                        }
                        if (currentState.metadata().hasIndex(indexMetadata.getIndex().getName())) {
                            continue;
                        }
                        if (currentState.metadata().hasAlias(indexMetadata.getIndex().getName())) {
                            logger.warn("ignoring dangled index [{}] on node [{}] due to an existing alias with the same name",
                                    indexMetadata.getIndex(), request.fromNode);
                            continue;
                        }
                        if (currentState.metadata().indexGraveyard().containsIndex(indexMetadata.getIndex())) {
                            logger.warn("ignoring dangled index [{}] on node [{}] since it was recently deleted",
                                    indexMetadata.getIndex(), request.fromNode);
                            continue;
                        }
                        importNeeded = true;

                        IndexMetadata newIndexMetadata;
                        try {
                            // The dangled index might be from an older version, we need to make sure it's compatible
                            // with the current version.
                            newIndexMetadata = indexMetadataVerifier.verifyIndexMetadata(indexMetadata,
                                minIndexCompatibilityVersion);
                            newIndexMetadata = IndexMetadata.builder(newIndexMetadata).settings(
                                Settings.builder().put(newIndexMetadata.getSettings()).put(
                                    IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())).build();
                        } catch (Exception ex) {
                            // upgrade failed - adding index as closed
                            logger.warn(() -> new ParameterizedMessage("found dangled index [{}] on node [{}]. This index cannot be " +
                                "upgraded to the latest version, adding as closed", indexMetadata.getIndex(), request.fromNode), ex);
                            newIndexMetadata = IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE)
                                .version(indexMetadata.getVersion() + 1).build();
                        }
                        metadata.put(newIndexMetadata, false);
                        blocks.addBlocks(newIndexMetadata);
                        if (newIndexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                            routingTableBuilder.addAsFromDangling(newIndexMetadata);
                        }
                        sb.append("[").append(newIndexMetadata.getIndex()).append("/").append(newIndexMetadata.getState())
                            .append("]");
                    }
                    if (importNeeded == false) {
                        return currentState;
                    }
                    logger.info("importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    RoutingTable routingTable = routingTableBuilder.build();
                    ClusterState updatedState = ClusterState.builder(currentState).metadata(metadata).blocks(blocks)
                        .routingTable(routingTable).build();

                    // now, reroute
                    return allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(routingTable).build(), "dangling indices allocated");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed send response for allocating dangled", inner);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        channel.sendResponse(new AllocateDangledResponse());
                    } catch (IOException e) {
                        logger.warn("failed send response for allocating dangled", e);
                    }
                }
            });
        }
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
            if (in.getVersion().before(Version.V_8_0_0)) {
                in.readBoolean();
            }
        }

        private AllocateDangledResponse() {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_8_0_0)) {
                out.writeBoolean(true);
            }
        }
    }
}
