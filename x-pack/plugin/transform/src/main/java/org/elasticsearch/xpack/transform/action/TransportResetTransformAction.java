/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.ResetTransformAction;
import org.elasticsearch.xpack.core.transform.action.ResetTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.transform.TransformExtensionHolder;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportResetTransformAction extends AcknowledgedTransportMasterNodeAction<Request> {

    private static final Logger logger = LogManager.getLogger(TransportResetTransformAction.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor auditor;
    private final Client client;
    private final SecurityContext securityContext;
    private final Settings settings;
    private final Settings destIndexSettings;

    @Inject
    public TransportResetTransformAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        Client client,
        Settings settings,
        TransformExtensionHolder transformExtensionHolder
    ) {
        super(
            ResetTransformAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transformConfigManager = transformServices.configManager();
        this.auditor = transformServices.auditor();
        this.client = Objects.requireNonNull(client);
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.settings = settings;
        this.destIndexSettings = transformExtensionHolder.getTransformExtension().getTransformDestinationIndexSettings();
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        if (TransformMetadata.upgradeMode(state)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot reset any Transform while the Transform feature is upgrading.",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        final boolean transformIsRunning = TransformTask.getTransformTask(request.getId(), state) != null;
        if (transformIsRunning && request.isForce() == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot reset transform [" + request.getId() + "] as the task is running. Stop the task first",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        // <4> Reset transform
        ActionListener<TransformUpdater.UpdateResult> updateTransformListener = ActionListener.wrap(
            unusedUpdateResult -> transformConfigManager.resetTransform(request.getId(), ActionListener.wrap(resetResponse -> {
                logger.info("[{}] reset transform", request.getId());
                auditor.info(request.getId(), "Reset transform.");
                listener.onResponse(AcknowledgedResponse.of(resetResponse));
            }, listener::onFailure)),
            listener::onFailure
        );

        // <3> Upgrade transform to the latest version
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> deleteDestIndexListener = ActionListener.wrap(
            transformConfigAndVersion -> {
                final ClusterState clusterState = clusterService.state();
                TransformUpdater.updateTransform(
                    securityContext,
                    indexNameExpressionResolver,
                    clusterState,
                    settings,
                    client,
                    transformConfigManager,
                    auditor,
                    transformConfigAndVersion.v1(),
                    TransformConfigUpdate.EMPTY,
                    transformConfigAndVersion.v2(),
                    false, // defer validation
                    false, // dry run
                    false, // check access
                    request.ackTimeout(),
                    destIndexSettings,
                    updateTransformListener
                );
            },
            listener::onFailure
        );

        // <2> Delete destination index if it was created by the transform
        ActionListener<StopTransformAction.Response> stopTransformActionListener = ActionListener.wrap(
            unusedStopResponse -> deleteDestinationIndexIfCreatedByTheTransform(request.getId(), deleteDestIndexListener),
            listener::onFailure
        );

        // <1> Stop transform if it's currently running
        if (transformIsRunning == false) {
            stopTransformActionListener.onResponse(null);
            return;
        }
        StopTransformAction.Request stopTransformRequest = new StopTransformAction.Request(
            request.getId(),
            true,
            request.isForce(),
            null,
            true,
            false
        );
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, StopTransformAction.INSTANCE, stopTransformRequest, stopTransformActionListener);
    }

    private void deleteDestinationIndexIfCreatedByTheTransform(
        String transformId,
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> listener
    ) {
        final SetOnce<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> transformConfigAndVersionHolder = new SetOnce<>();

        // <4> Send the fetched config to the caller
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
            unusedDeleteIndexResponse -> listener.onResponse(transformConfigAndVersionHolder.get()),
            listener::onFailure
        );

        // <3> Delete destination index if it was created by transform
        ActionListener<Boolean> isDestinationIndexCreatedByTransformListener = ActionListener.wrap(isDestinationIndexCreatedByTransform -> {
            if (isDestinationIndexCreatedByTransform == false) {
                // Destination index was created outside of transform, we don't delete it and just move on.
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            String destIndex = transformConfigAndVersionHolder.get().v1().getDestination().getIndex();
            DeleteIndexRequest deleteDestIndexRequest = new DeleteIndexRequest(destIndex);
            executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, TransportDeleteIndexAction.TYPE, deleteDestIndexRequest, finalListener);
        }, listener::onFailure);

        // <2> Check if the destination index was created by transform
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> getTransformConfigurationListener = ActionListener.wrap(
            transformConfigAndVersion -> {
                transformConfigAndVersionHolder.set(transformConfigAndVersion);
                String destIndex = transformConfigAndVersion.v1().getDestination().getIndex();
                TransformIndex.isDestinationIndexCreatedByTransform(client, destIndex, isDestinationIndexCreatedByTransformListener);
            },
            listener::onFailure
        );

        // <1> Fetch transform configuration
        transformConfigManager.getTransformConfigurationForUpdate(transformId, getTransformConfigurationListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
