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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.inference.InferenceIndex;

public class TransportDeleteRegionPolicyAction extends HandledTransportAction<DeleteRegionPolicyAction.Request, AcknowledgedResponse> {

    private final OriginSettingClient client;

    @Inject
    public TransportDeleteRegionPolicyAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            DeleteRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            DeleteRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
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
