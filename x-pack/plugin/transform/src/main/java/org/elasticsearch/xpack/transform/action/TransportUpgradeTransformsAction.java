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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.PageParams;
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
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
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
        this(
            UpgradeTransformsAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            transformServices,
            client,
            settings
        );
    }

    protected TransportUpgradeTransformsAction(
        String name,
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
            name,
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

    private void updateOneTransform(String id, boolean dryRun, ActionListener<UpdateResult> listener) {
        final ClusterState clusterState = clusterService.state();

        transformConfigManager.getTransformConfigurationForUpdate(id, ActionListener.wrap(configAndVersion -> {
            TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null);
            TransformConfig config = configAndVersion.v1();

            // keep headers, todo: rewrite old data_frame roles?
            update.setHeaders(config.getHeaders());

            TransformUpdater.updateTransform(
                securityContext,
                indexNameExpressionResolver,
                clusterState,
                settings,
                client,
                transformConfigManager,
                config,
                update,
                configAndVersion.v2(),
                false,
                dryRun,
                false, // check access,
                listener
            );
        }, listener::onFailure));
    }

    private void recursiveUpdate(
        Deque<String> transformsToUpgrade,
        Map<UpdateResult.Status, Long> updatesByStatus,
        boolean dryRun,
        ActionListener<Void> listener
    ) {
        String next = transformsToUpgrade.pop();
        updateOneTransform(next, dryRun, ActionListener.wrap(updateResponse -> {
            TransformConfig updatedConfig = updateResponse.getConfig();
            auditor.info(updatedConfig.getId(), "Updated transform.");
            logger.debug("[{}] Updated transform [{}]", updatedConfig.getId(), updateResponse.getStatus());
            updatesByStatus.compute(updateResponse.getStatus(), (k, v) -> (v == null) ? 1 : v + 1L);

            if (transformsToUpgrade.isEmpty() == false) {
                recursiveUpdate(transformsToUpgrade, updatesByStatus, dryRun, listener);
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private void recursiveExpandTransformIdsAndUpgrade(
        Map<UpdateResult.Status, Long> updatesByStatus,
        boolean dryRun,
        PageParams page,
        ActionListener<Map<UpdateResult.Status, Long>> listener
    ) {
        transformConfigManager.expandTransformIds(Metadata.ALL, page, true, ActionListener.wrap(hitsAndIds -> {

            List<String> ids = hitsAndIds.v2().v1();
            Deque<String> transformsToUpgrade = new ArrayDeque<>(ids);

            recursiveUpdate(transformsToUpgrade, updatesByStatus, dryRun, ActionListener.wrap(r -> {
                if (hitsAndIds.v1().intValue() >= (page.getSize())) {
                    PageParams nextPage = new PageParams(page.getFrom() + page.getSize(), PageParams.DEFAULT_SIZE);
                    recursiveExpandTransformIdsAndUpgrade(updatesByStatus, dryRun, nextPage, listener);
                    return;
                }
                listener.onResponse(updatesByStatus);
            }, listener::onFailure)

            );
        }, listener::onFailure));
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

        PageParams startPage = new PageParams(0, PageParams.DEFAULT_SIZE);
        Map<UpdateResult.Status, Long> updatesByStatus = new HashMap<>();

        recursiveExpandTransformIdsAndUpgrade(updatesByStatus, request.isDryRun(), startPage, ActionListener.wrap(r -> {
            if (request.isDryRun() == false) {
                transformConfigManager.deleteOldIndices(state, ActionListener.wrap(deletedIndices -> {
                    logger.debug("Deleted [{}] old transform internal indices", deletedIndices);

                    long updated = updatesByStatus.containsKey(UpdateResult.Status.UPDATED)
                        ? updatesByStatus.get(UpdateResult.Status.UPDATED)
                        : 0;

                    long skipped = updatesByStatus.containsKey(UpdateResult.Status.NONE)
                        ? updatesByStatus.get(UpdateResult.Status.NONE)
                        : 0;

                    logger.info("Successfully upgraded all transforms, (updated: [{}], skipped [{}])", updated, skipped);

                    listener.onResponse(
                        new UpgradeTransformsAction.Response(
                            true,
                            updatesByStatus.get(UpdateResult.Status.UPDATED),
                            updatesByStatus.get(UpdateResult.Status.NONE),
                            null
                        )
                    );
                }, listener::onFailure));
            } else {
                listener.onResponse(
                    new UpgradeTransformsAction.Response(true, null, null, updatesByStatus.get(UpdateResult.Status.NEEDS_UPDATE))
                );
            }
        }, listener::onFailure));

    }

    @Override
    protected ClusterBlockException checkBlock(UpgradeTransformsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
