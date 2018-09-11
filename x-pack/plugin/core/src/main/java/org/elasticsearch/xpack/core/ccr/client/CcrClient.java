/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowIndexAction;

import java.util.Objects;

public class CcrClient {

    private final ElasticsearchClient client;

    public CcrClient(final ElasticsearchClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public void createAndFollow(
            final CreateAndFollowIndexAction.Request request,
            final ActionListener<CreateAndFollowIndexAction.Response> listener) {
        client.execute(CreateAndFollowIndexAction.INSTANCE, request, listener);
    }

    public ActionFuture<CreateAndFollowIndexAction.Response> createAndFollow(final CreateAndFollowIndexAction.Request request) {
        final PlainActionFuture<CreateAndFollowIndexAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(CreateAndFollowIndexAction.INSTANCE, request, listener);
        return listener;
    }

    public void follow(final FollowIndexAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        client.execute(FollowIndexAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> follow(final FollowIndexAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(FollowIndexAction.INSTANCE, request, listener);
        return listener;
    }

    public void stats(
            final CcrStatsAction.TasksRequest request,
            final ActionListener<CcrStatsAction.TasksResponse> listener) {
        client.execute(CcrStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<CcrStatsAction.TasksResponse> stats(final CcrStatsAction.TasksRequest request) {
        final PlainActionFuture<CcrStatsAction.TasksResponse> listener = PlainActionFuture.newFuture();
        client.execute(CcrStatsAction.INSTANCE, request, listener);
        return listener;
    }

    public void unfollow(final UnfollowIndexAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        client.execute(UnfollowIndexAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> unfollow(final UnfollowIndexAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(UnfollowIndexAction.INSTANCE, request, listener);
        return listener;
    }

}
