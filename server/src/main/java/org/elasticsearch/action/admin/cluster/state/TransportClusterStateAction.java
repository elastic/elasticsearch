/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class TransportClusterStateAction extends TransportMasterNodeReadAction<ClusterStateRequest, ClusterStateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterStateAction.class);

    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportClusterStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver
    ) {
        super(
            ClusterStateAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterStateRequest::new,
            ClusterStateResponse::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterStateRequest request,
        final ClusterState state,
        final ActionListener<ClusterStateResponse> listener
    ) throws IOException {

        assert task instanceof CancellableTask : task + " not cancellable";
        final CancellableTask cancellableTask = (CancellableTask) task;

        final Predicate<ClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? Predicates.always()
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ClusterState> acceptableClusterStateOrFailedPredicate = request.local()
            ? acceptableClusterStatePredicate
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedMaster() == false);

        if (cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrFailedPredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (cancellableTask.notifyIfCancelled(listener)) {
                            return;
                        }

                        if (acceptableClusterStatePredicate.test(newState)) {
                            executor.execute(ActionRunnable.supply(listener, () -> buildResponse(request, newState)));
                        } else {
                            listener.onFailure(
                                new NotMasterException(
                                    "master stepped down waiting for metadata version " + request.waitForMetadataVersion()
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        ActionListener.run(listener, l -> {
                            if (cancellableTask.notifyIfCancelled(l) == false) {
                                l.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                            }
                        });
                    }
                }, clusterState -> cancellableTask.isCancelled() || acceptableClusterStateOrFailedPredicate.test(clusterState));
        }
    }

    private ClusterState filterClusterState(final ClusterState inputState) {
        final Collection<ProjectId> projectIds = projectResolver.getProjectIds(inputState);
        final Metadata metadata = inputState.metadata();
        if (projectIds.containsAll(metadata.projects().keySet())
            && projectIds.containsAll(inputState.globalRoutingTable().routingTables().keySet())) {
            // no filtering required - everything in the cluster state is within the set of projects
            return inputState;
        }
        final Metadata.Builder mdBuilder = Metadata.builder(inputState.metadata());
        final GlobalRoutingTable.Builder rtBuilder = GlobalRoutingTable.builder(inputState.globalRoutingTable());
        for (var projectId : metadata.projects().keySet()) {
            if (projectIds.contains(projectId) == false) {
                mdBuilder.removeProject(projectId);
                rtBuilder.removeProject(projectId);
            }
        }
        return ClusterState.builder(inputState).metadata(mdBuilder.build()).routingTable(rtBuilder.build()).build();
    }

    @SuppressForbidden(reason = "exposing ClusterState#compatibilityVersions requires reading them")
    private static Map<String, CompatibilityVersions> getCompatibilityVersions(ClusterState clusterState) {
        return clusterState.compatibilityVersions();
    }

    @SuppressForbidden(reason = "exposing ClusterState#clusterFeatures requires reading them")
    private static Map<String, Set<String>> getClusterFeatures(ClusterState clusterState) {
        return clusterState.clusterFeatures().nodeFeatures();
    }

    private ClusterStateResponse buildResponse(final ClusterStateRequest request, final ClusterState rawState) {
        final ClusterState currentState = filterClusterState(rawState);

        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT); // too heavy to construct & serialize cluster state without forking

        if (request.blocks() == false) {
            final var blockException = currentState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
            if (blockException != null) {
                // There's a METADATA_READ block in place, but we aren't returning it to the caller, and yet the caller needs to know that
                // this block exists (e.g. it's the STATE_NOT_RECOVERED_BLOCK, so the rest of the state is known to be incomplete). Thus we
                // must fail the request:
                throw blockException;
            }
        }

        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());
        builder.projectsSettings(currentState.projectsSettings());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
            builder.nodeIdsToCompatibilityVersions(getCompatibilityVersions(currentState));
            builder.nodeFeatures(getClusterFeatures(currentState));
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                final GlobalRoutingTable.Builder globalRoutingTableBuilder = GlobalRoutingTable.builder(currentState.globalRoutingTable())
                    .clear();
                for (ProjectMetadata project : currentState.metadata().projects().values()) {
                    RoutingTable projectRouting = currentState.routingTable(project.id());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                    String[] indices = indexNameExpressionResolver.concreteIndexNames(project, request);
                    for (String filteredIndex : indices) {
                        if (projectRouting.hasIndex(filteredIndex)) {
                            routingTableBuilder.add(projectRouting.getIndicesRouting().get(filteredIndex));
                        }
                    }
                    globalRoutingTableBuilder.put(project.id(), routingTableBuilder);
                }
                builder.routingTable(globalRoutingTableBuilder.build());
            } else {
                builder.routingTable(currentState.globalRoutingTable());
            }
        } else {
            builder.routingTable(GlobalRoutingTable.builder().build());
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        if (request.metadata()) {
            // filter out metadata that shouldn't be returned by the API
            final BiPredicate<String, Metadata.MetadataCustom<?>> notApi = (ignore, custom) -> custom.context()
                .contains(Metadata.XContentContext.API) == false;
            if (request.indices().length > 0) {
                // if the request specified index names, then we don't want the whole metadata, just the version and projects (which will
                // be filtered (below) to only include the relevant indices)
                mdBuilder.version(currentState.metadata().version());
            } else {
                // If there are no requested indices, then we want all the metadata, except for customs that aren't exposed via the API
                mdBuilder = Metadata.builder(currentState.metadata());
                mdBuilder.removeCustomIf(notApi);
            }

            for (ProjectMetadata project : currentState.metadata().projects().values()) {
                ProjectMetadata.Builder pBuilder;
                if (request.indices().length > 0) {
                    // if the request specified index names, then only include the project-id and indices
                    pBuilder = ProjectMetadata.builder(project.id());
                    String[] indices = indexNameExpressionResolver.concreteIndexNames(project, request);
                    for (String filteredIndex : indices) {
                        // If the requested index is part of a data stream then that data stream should also be included:
                        IndexAbstraction indexAbstraction = project.getIndicesLookup().get(filteredIndex);
                        if (indexAbstraction.getParentDataStream() != null) {
                            DataStream dataStream = indexAbstraction.getParentDataStream();
                            // Also the IMD of other backing indices need to be included, otherwise the cluster state api
                            // can't create a valid cluster state instance:
                            for (Index backingIndex : dataStream.getIndices()) {
                                pBuilder.put(project.index(backingIndex), false);
                            }
                            pBuilder.put(dataStream);
                        } else {
                            IndexMetadata indexMetadata = project.index(filteredIndex);
                            if (indexMetadata != null) {
                                pBuilder.put(indexMetadata, false);
                            }
                        }
                    }
                } else {
                    // if the request did not specify index names, then include everything from the project except non-API customs
                    pBuilder = ProjectMetadata.builder(project);
                    pBuilder.removeCustomIf(notApi);
                }
                mdBuilder.put(pBuilder);
            }
        } else {
            for (ProjectId project : currentState.metadata().projects().keySet()) {
                // Request doesn't want to retrieve metadata, so we just fill in empty projects
                // (because we can't have a truly empty Metadata)
                mdBuilder.put(ProjectMetadata.builder(project));
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (Map.Entry<String, ClusterState.Custom> custom : currentState.customs().entrySet()) {
                if (custom.getValue().isPrivate() == false) {
                    builder.putCustom(custom.getKey(), custom.getValue());
                }
            }
        }

        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }

}
