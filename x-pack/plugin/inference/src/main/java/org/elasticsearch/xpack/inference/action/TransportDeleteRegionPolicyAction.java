/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.audit.InferenceAuditEventDoc;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.audit.InferenceAuditService;

import java.time.Instant;
import java.util.Optional;

public class TransportDeleteRegionPolicyAction extends HandledTransportAction<DeleteRegionPolicyAction.Request, AcknowledgedResponse> {

    private final OriginSettingClient client;
    private final InferenceAuditService auditService;
    private final Optional<SecurityContext> securityContext;

    @Inject
    public TransportDeleteRegionPolicyAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        InferenceAuditService auditService
    ) {
        super(
            DeleteRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            DeleteRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.auditService = auditService;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? Optional.of(new SecurityContext(settings, threadPool.getThreadContext()))
            : Optional.empty();
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
                        listener.onResponse(AcknowledgedResponse.TRUE);
                        String username = securityContext.map(ctx -> ctx.getUser()).map(user -> user.principal()).orElse(null);
                        auditService.auditEvent(
                            new InferenceAuditEventDoc(
                                Instant.now(),
                                InferenceAuditEventDoc.Action.DELETE,
                                InferenceAuditEventDoc.ResourceType.REGION_POLICY,
                                RegionPolicyDoc.DOCUMENT_ID,
                                username
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
}
