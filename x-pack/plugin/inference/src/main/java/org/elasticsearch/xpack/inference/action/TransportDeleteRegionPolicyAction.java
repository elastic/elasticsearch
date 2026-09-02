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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;

import java.util.Optional;

public class TransportDeleteRegionPolicyAction extends HandledTransportAction<DeleteRegionPolicyAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteRegionPolicyAction.class);

    private final OriginSettingClient client;
    private final Optional<SecurityContext> securityContext;
    private final InferencePreferencesCache inferencePreferencesCache;

    @Inject
    public TransportDeleteRegionPolicyAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        InferencePreferencesCache inferencePreferencesCache
    ) {
        super(
            DeleteRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            DeleteRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? Optional.of(new SecurityContext(settings, threadPool.getThreadContext()))
            : Optional.empty();
        this.inferencePreferencesCache = inferencePreferencesCache;
    }

    @Override
    protected void doExecute(Task task, DeleteRegionPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String username = securityContext.map(ctx -> ctx.getUser()).map(user -> user.principal()).orElse(null);
        client.prepareDelete(InferenceIndex.INDEX_NAME, RegionPolicyDoc.DOCUMENT_ID)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        listener.onFailure(TransportGetRegionPolicyAction.noRegionPolicyConfiguredException());
                    } else {
                        logger.info("Region policy deleted by [{}]", username);
                        inferencePreferencesCache.invalidate(
                            ActionListener.runAfter(
                                ActionListener.wrap(
                                    ignored -> {},
                                    e -> logger.warn("Failed to invalidate inference preferences cache after deleting region policy", e)
                                ),
                                () -> refreshAuthorizedEndpointsAndRespond(listener)
                            )
                        );
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

    private void refreshAuthorizedEndpointsAndRespond(ActionListener<AcknowledgedResponse> listener) {
        client.execute(
            RefreshAuthorizedEndpointsAction.INSTANCE,
            new RefreshAuthorizedEndpointsAction.Request(),
            ActionListener.runAfter(
                ActionListener.<ActionResponse.Empty>wrap(
                    ignored -> {},
                    e -> logger.warn("Failed to refresh authorized endpoints after deleting region policy", e)
                ),
                () -> listener.onResponse(AcknowledgedResponse.TRUE)
            )
        );
    }

}
