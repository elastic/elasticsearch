/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Objects;

public class EnrichClient {

     private final ElasticsearchClient client;

    public EnrichClient(ElasticsearchClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public void deleteEnrichPolicy(
        final DeleteEnrichPolicyAction.Request request,
        final ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteEnrichPolicyAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteEnrichPolicy(final DeleteEnrichPolicyAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteEnrichPolicyAction.INSTANCE, request, listener);
        return listener;
    }

    public void executeEnrichPolicy(
        final ExecuteEnrichPolicyAction.Request request,
        final ActionListener<ExecuteEnrichPolicyAction.Response> listener) {
        client.execute(ExecuteEnrichPolicyAction.INSTANCE, request, listener);
    }

    public ActionFuture<ExecuteEnrichPolicyAction.Response> executeEnrichPolicy(final ExecuteEnrichPolicyAction.Request request) {
        final PlainActionFuture<ExecuteEnrichPolicyAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(ExecuteEnrichPolicyAction.INSTANCE, request, listener);
        return listener;
    }

    public void getEnrichPolicy(
        final GetEnrichPolicyAction.Request request,
        final ActionListener<GetEnrichPolicyAction.Response> listener) {
        client.execute(GetEnrichPolicyAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetEnrichPolicyAction.Response> getEnrichPolicy(final GetEnrichPolicyAction.Request request) {
        final PlainActionFuture<GetEnrichPolicyAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetEnrichPolicyAction.INSTANCE, request, listener);
        return listener;
    }

    public void putEnrichPolicy(
        final PutEnrichPolicyAction.Request request,
        final ActionListener<AcknowledgedResponse> listener) {
        client.execute(PutEnrichPolicyAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> putEnrichPolicy(final PutEnrichPolicyAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(PutEnrichPolicyAction.INSTANCE, request, listener);
        return listener;
    }
}
