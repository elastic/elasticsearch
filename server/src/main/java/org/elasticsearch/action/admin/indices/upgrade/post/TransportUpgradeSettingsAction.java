/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportUpgradeSettingsAction extends AcknowledgedTransportMasterNodeAction<UpgradeSettingsRequest> {

    private static final Logger logger = LogManager.getLogger(TransportUpgradeSettingsAction.class);

    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpgradeSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataUpdateSettingsService updateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionFilters actionFilters
    ) {
        super(
            UpgradeSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpgradeSettingsRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected ClusterBlockException checkBlock(UpgradeSettingsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        final UpgradeSettingsRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        UpgradeSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpgradeSettingsClusterStateUpdateRequest().ackTimeout(
            request.timeout()
        ).versions(request.versions()).masterNodeTimeout(request.masterNodeTimeout());

        updateSettingsService.upgradeIndexSettings(clusterStateUpdateRequest, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "failed to upgrade minimum compatibility version settings on indices [{}]",
                        request.versions().keySet()
                    ),
                    t
                );
                listener.onFailure(t);
            }
        });
    }
}
