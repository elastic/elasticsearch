/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction.Request;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.action.TransformUpdater.UpdateResult;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class TransportUpgradeTransformsAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpgradeTransformsAction.class);
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final Client client;
    private final TransformAuditor auditor;

    @Inject
    public TransportUpgradeTransformsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        Client client,
        Settings settings
    ) {
        super(
            UpgradeTransformsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.transformConfigManager = transformServices.getConfigManager();
        this.settings = settings;

        this.client = client;
        this.auditor = transformServices.getAuditor();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
    }

    @Override
    protected void masterOperation(Task ignoredTask, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception {
        TransformNodes.warnIfNoTransformNodes(state);

        // do not allow in mixed clusters
        if (state.nodes().getMaxNodeVersion().after(state.nodes().getMinNodeVersion())) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot upgrade transforms. All nodes must be the same version [{}]",
                    RestStatus.CONFLICT,
                    state.nodes().getMaxNodeVersion().toString()
                )
            );
            return;
        }

        recursiveExpandTransformIdsAndUpgrade(request.isDryRun(), request.timeout(), ActionListener.wrap(updatesByStatus -> {
            final long updated = updatesByStatus.getOrDefault(UpdateResult.Status.UPDATED, 0L);
            final long noAction = updatesByStatus.getOrDefault(UpdateResult.Status.NONE, 0L);
            final long needsUpdate = updatesByStatus.getOrDefault(UpdateResult.Status.NEEDS_UPDATE, 0L);

            if (request.isDryRun() == false) {
                transformConfigManager.deleteOldIndices(ActionListener.wrap(aBool -> {
                    logger.info("Successfully upgraded all transforms, (updated: [{}], no action [{}])", updated, noAction);

                    listener.onResponse(new UpgradeTransformsAction.Response(updated, noAction, needsUpdate));
                }, listener::onFailure));
            } else {
                // else: dry run
                listener.onResponse(new UpgradeTransformsAction.Response(updated, noAction, needsUpdate));
            }
        }, listener::onFailure));

    }

    @Override
    protected ClusterBlockException checkBlock(UpgradeTransformsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void updateOneTransform(String id, boolean dryRun, TimeValue timeout, ActionListener<UpdateResult> listener) {
        final ClusterState clusterState = clusterService.state();

        transformConfigManager.getTransformConfigurationForUpdate(id, ActionListener.wrap(configAndVersion -> {
            TransformConfigUpdate update = TransformConfigUpdate.EMPTY;
            TransformConfig config = configAndVersion.v1();

            /*
             * keep headers from the original document
             *
             * TODO: Handle deprecated data_frame_transform roles
             *
             * The headers store user roles and in case the transform has been created in 7.2-7.4
             * contain the old data_frame_transform_* roles
             *
             * For 9.x we need to take action as data_frame_transform_* will be removed
             *
             * Hint: {@link AuthenticationContextSerializer} for decoding the header
             */
            update.setHeaders(config.getHeaders());

            TransformUpdater.updateTransform(
                securityContext,
                indexNameExpressionResolver,
                clusterState,
                settings,
                client,
                transformConfigManager,
                auditor,
                config,
                update,
                configAndVersion.v2(),
                false, // defer validation
                dryRun,
                false, // check access,
                timeout,
                listener
            );
        }, failure -> {
            // ignore if transform got deleted while upgrade was running
            if (failure instanceof ResourceNotFoundException) {
                listener.onResponse(new UpdateResult(null, null, UpdateResult.Status.DELETED));
            } else {
                listener.onFailure(failure);
            }
        }));
    }

    private void recursiveUpdate(
        Deque<String> transformsToUpgrade,
        Map<UpdateResult.Status, Long> updatesByStatus,
        boolean dryRun,
        TimeValue timeout,
        ActionListener<Void> listener
    ) {
        String next = transformsToUpgrade.pollFirst();

        // extra paranoia: return if next is null
        if (next == null) {
            listener.onResponse(null);
            return;
        }

        updateOneTransform(next, dryRun, timeout, ActionListener.wrap(updateResponse -> {
            if (UpdateResult.Status.DELETED.equals(updateResponse.getStatus()) == false) {
                auditor.info(next, "Updated transform.");
                logger.debug("[{}] Updated transform [{}]", next, updateResponse.getStatus());
                updatesByStatus.compute(updateResponse.getStatus(), (k, v) -> (v == null) ? 1 : v + 1L);
            }
            if (transformsToUpgrade.isEmpty() == false) {
                recursiveUpdate(transformsToUpgrade, updatesByStatus, dryRun, timeout, listener);
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private void recursiveExpandTransformIdsAndUpgrade(
        boolean dryRun,
        TimeValue timeout,
        ActionListener<Map<UpdateResult.Status, Long>> listener
    ) {
        transformConfigManager.getAllOutdatedTransformIds(timeout, ActionListener.wrap(totalAndIds -> {

            // exit quickly if there is nothing to do
            if (totalAndIds.v2().isEmpty()) {
                listener.onResponse(Collections.singletonMap(UpdateResult.Status.NONE, totalAndIds.v1()));
                return;
            }

            Map<UpdateResult.Status, Long> updatesByStatus = new HashMap<>();
            updatesByStatus.put(UpdateResult.Status.NONE, totalAndIds.v1() - totalAndIds.v2().size());

            Deque<String> ids = new ArrayDeque<>(totalAndIds.v2());

            recursiveUpdate(
                ids,
                updatesByStatus,
                dryRun,
                timeout,
                ActionListener.wrap(r -> listener.onResponse(updatesByStatus), listener::onFailure)
            );
        }, listener::onFailure));
    }
}
