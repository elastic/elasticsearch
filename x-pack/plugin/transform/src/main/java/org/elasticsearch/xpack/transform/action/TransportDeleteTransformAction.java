/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;

public class TransportDeleteTransformAction extends AcknowledgedTransportMasterNodeAction<Request> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteTransformAction.class);

    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor auditor;
    private final Client client;

    @Inject
    public TransportDeleteTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client
    ) {
        super(
            DeleteTransformAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.transformConfigManager = transformServices.configManager();
        this.auditor = transformServices.auditor();
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        if (TransformMetadata.upgradeMode(state)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot delete any Transform while the Transform feature is upgrading.",
                    RestStatus.CONFLICT
                )
            );
            return;
        }
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final boolean transformIsRunning = TransformTask.getTransformTask(request.getId(), state) != null;
        if (transformIsRunning && request.isForce() == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot delete transform [" + request.getId() + "] as the task is running. Stop the task first",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        // <3> Delete transform config
        ActionListener<AcknowledgedResponse> deleteDestIndexListener = ActionListener.wrap(
            unusedAcknowledgedResponse -> transformConfigManager.deleteTransform(request.getId(), ActionListener.wrap(r -> {
                logger.info("[{}] deleted transform", request.getId());
                auditor.info(request.getId(), "Deleted transform.");
                listener.onResponse(AcknowledgedResponse.of(r));
            }, listener::onFailure)),
            listener::onFailure
        );

        // <2> Delete destination index if requested
        ActionListener<StopTransformAction.Response> stopTransformActionListener = ActionListener.wrap(unusedStopResponse -> {
            if (request.isDeleteDestIndex()) {
                deleteDestinationIndex(parentTaskId, request.getId(), request.ackTimeout(), deleteDestIndexListener);
            } else {
                deleteDestIndexListener.onResponse(null);
            }
        }, listener::onFailure);

        // <1> Stop transform if it's currently running
        stopTransform(transformIsRunning, parentTaskId, request.getId(), request.ackTimeout(), stopTransformActionListener);
    }

    private void stopTransform(
        boolean transformIsRunning,
        TaskId parentTaskId,
        String transformId,
        TimeValue timeout,
        ActionListener<StopTransformAction.Response> listener
    ) {
        if (transformIsRunning == false) {
            listener.onResponse(null);
            return;
        }
        StopTransformAction.Request stopTransformRequest = new StopTransformAction.Request(transformId, true, true, timeout, true, false);
        stopTransformRequest.setParentTask(parentTaskId);
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, StopTransformAction.INSTANCE, stopTransformRequest, listener);
    }

    private void deleteDestinationIndex(
        TaskId parentTaskId,
        String transformId,
        TimeValue timeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        getTransformConfig(transformId).<AcknowledgedResponse>andThen((l, r) -> deleteDestinationIndex(r.v1(), parentTaskId, timeout, l))
            .addListener(listener.delegateResponse((l, e) -> {
                if (e instanceof IndexNotFoundException) {
                    l.onResponse(AcknowledgedResponse.TRUE);
                } else {
                    l.onFailure(e);
                }
            }));
    }

    private SubscribableListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> getTransformConfig(String transformId) {
        return SubscribableListener.newForked(l -> transformConfigManager.getTransformConfigurationForUpdate(transformId, l));
    }

    /**
     * Delete the destination index.  If the Transform is configured to write to an alias, then follow that alias to the concrete index.
     */
    private void deleteDestinationIndex(
        TransformConfig config,
        TaskId parentTaskId,
        TimeValue timeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        SubscribableListener.<String>newForked(l -> resolveDestinationIndex(config, parentTaskId, timeout, l))
            .<AcknowledgedResponse>andThen((l, destIndex) -> {
                DeleteIndexRequest deleteDestIndexRequest = new DeleteIndexRequest(destIndex);
                deleteDestIndexRequest.ackTimeout(timeout);
                deleteDestIndexRequest.setParentTask(parentTaskId);
                executeWithHeadersAsync(
                    config.getHeaders(),
                    TRANSFORM_ORIGIN,
                    client,
                    TransportDeleteIndexAction.TYPE,
                    deleteDestIndexRequest,
                    l
                );
            })
            .addListener(listener);
    }

    private void resolveDestinationIndex(TransformConfig config, TaskId parentTaskId, TimeValue timeout, ActionListener<String> listener) {
        var destIndex = config.getDestination().getIndex();
        var responseListener = ActionListener.<GetAliasesResponse>wrap(r -> findDestinationIndexInAliases(r, destIndex, listener), e -> {
            if (e instanceof AliasesNotFoundException) {
                // no alias == the destIndex is our concrete index
                listener.onResponse(destIndex);
            } else {
                listener.onFailure(e);
            }
        });

        GetAliasesRequest request = new GetAliasesRequest(timeout, destIndex);
        request.setParentTask(parentTaskId);
        executeWithHeadersAsync(config.getHeaders(), TRANSFORM_ORIGIN, client, GetAliasesAction.INSTANCE, request, responseListener);
    }

    private static void findDestinationIndexInAliases(GetAliasesResponse aliases, String destIndex, ActionListener<String> listener) {
        var indexToAliases = aliases.getAliases();
        if (indexToAliases.isEmpty()) {
            // if the alias list is empty, that means the index is a concrete index
            listener.onResponse(destIndex);
        } else if (indexToAliases.size() == 1) {
            // if there is one value, the alias will treat it as the write index, so it's our destination index
            listener.onResponse(indexToAliases.keySet().iterator().next());
        } else {
            // if there is more than one index, there may be more than one alias for each index
            // we have to search for the alias that matches our destination index name AND is declared the write index for that alias
            indexToAliases.entrySet().stream().map(entry -> {
                if (entry.getValue().stream().anyMatch(md -> destIndex.equals(md.getAlias()) && Boolean.TRUE.equals(md.writeIndex()))) {
                    return entry.getKey();
                } else {
                    return null;
                }
            }).filter(Objects::nonNull).findFirst().ifPresentOrElse(listener::onResponse, () -> {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot disambiguate destination index alias ["
                            + destIndex
                            + "]. Alias points to many indices with no clear write alias. Retry with delete_dest_index=false and manually"
                            + " clean up destination index.",
                        RestStatus.CONFLICT
                    )
                );
            });
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
