/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.inference.InferenceIndex;

public class TransportDeleteRegionPolicyAction extends HandledTransportAction<DeleteRegionPolicyAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteRegionPolicyAction.class);
    private final OriginSettingClient client;
    private final RegionPolicySettings regionPolicySettings;

    @Inject
    public TransportDeleteRegionPolicyAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            DeleteRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            DeleteRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.regionPolicySettings = new RegionPolicySettings(settings);
    }

    @Override
    protected void doExecute(Task task, DeleteRegionPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        client.prepareDelete(InferenceIndex.INDEX_NAME, RegionPolicyDoc.DOCUMENT_ID)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        listener.onFailure(TransportGetRegionPolicyAction.noRegionPolicyConfiguredException());
                    } else {
                        refreshAuthorizationEndpoints(listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        listener.onFailure(TransportGetRegionPolicyAction.noRegionPolicyConfiguredException());
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
    }

    private void refreshAuthorizationEndpoints(ActionListener<AcknowledgedResponse> listener) {
        if (regionPolicySettings.skipAuthorizationRefresh()) {
            logger.debug("Skipping refresh of authorized endpoints after deleting region policy due to test setting");
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        var authListener = ActionListener.<ActionResponse.Empty>wrap(
            ignore -> listener.onResponse(AcknowledgedResponse.TRUE),
            // If the refresh fails, we don't want to fail the delete region policy request, so we ignore the exception and log it
            e -> {
                logger.warn("""
                    Failed to refresh authorized endpoints after deleting region policy. \
                    The new region policy will not take effect until the next authorization poll.""", e);
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        );

        client.execute(RefreshAuthorizedEndpointsAction.INSTANCE, new RefreshAuthorizedEndpointsAction.Request(), authListener);
    }
}
