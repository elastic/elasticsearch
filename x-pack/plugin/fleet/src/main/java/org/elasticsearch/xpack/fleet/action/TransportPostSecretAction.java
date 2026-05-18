/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;
import static org.elasticsearch.xpack.fleet.Fleet.FLEET_SECRETS_INDEX_NAME;

public class TransportPostSecretAction extends HandledTransportAction<PostSecretRequest, PostSecretResponse> {

    private final Client client;

    @Inject
    public TransportPostSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(PostSecretAction.NAME, transportService, actionFilters, PostSecretRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, FLEET_ORIGIN);
    }

    protected void doExecute(Task task, PostSecretRequest request, ActionListener<PostSecretResponse> listener) {
        try {
            client.prepareIndex(FLEET_SECRETS_INDEX_NAME)
                .setSource(request.toXContent(jsonBuilder()))
                .execute(
                    listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(new PostSecretResponse(indexResponse.getId())))
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
