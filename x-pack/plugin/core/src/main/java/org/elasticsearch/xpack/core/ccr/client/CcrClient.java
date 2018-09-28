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
import org.elasticsearch.xpack.core.ccr.action.AutoFollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;

import java.util.Objects;

public class CcrClient {

    private final ElasticsearchClient client;

    public CcrClient(final ElasticsearchClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public void putFollow(
            final PutFollowAction.Request request,
            final ActionListener<PutFollowAction.Response> listener) {
        client.execute(PutFollowAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutFollowAction.Response> putFollow(final PutFollowAction.Request request) {
        final PlainActionFuture<PutFollowAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(PutFollowAction.INSTANCE, request, listener);
        return listener;
    }

    public void resumeFollow(final ResumeFollowAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        client.execute(ResumeFollowAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> resumeFollow(final ResumeFollowAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(ResumeFollowAction.INSTANCE, request, listener);
        return listener;
    }

    public void stats(
            final CcrStatsAction.StatsRequest request,
            final ActionListener<CcrStatsAction.StatsResponses> listener) {
        client.execute(CcrStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<CcrStatsAction.StatsResponses> stats(final CcrStatsAction.StatsRequest request) {
        final PlainActionFuture<CcrStatsAction.StatsResponses> listener = PlainActionFuture.newFuture();
        client.execute(CcrStatsAction.INSTANCE, request, listener);
        return listener;
    }

    public void autoFollowStats(final AutoFollowStatsAction.Request request,
                                final ActionListener<AutoFollowStatsAction.Response> listener) {
        client.execute(AutoFollowStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<AutoFollowStatsAction.Response> autoFollowStats(final AutoFollowStatsAction.Request request) {
        final PlainActionFuture<AutoFollowStatsAction.Response> listener = PlainActionFuture.newFuture();
        autoFollowStats(request, listener);
        return listener;
    }

    public void pauseFollow(final PauseFollowAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        client.execute(PauseFollowAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> pauseFollow(final PauseFollowAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(PauseFollowAction.INSTANCE, request, listener);
        return listener;
    }

    public void putAutoFollowPattern(
            final PutAutoFollowPatternAction.Request request,
            final ActionListener<AcknowledgedResponse> listener) {
        client.execute(PutAutoFollowPatternAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> putAutoFollowPattern(final PutAutoFollowPatternAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(PutAutoFollowPatternAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteAutoFollowPattern(
            final DeleteAutoFollowPatternAction.Request request,
            final ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteAutoFollowPatternAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteAutoFollowPattern(final DeleteAutoFollowPatternAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteAutoFollowPatternAction.INSTANCE, request, listener);
        return listener;
    }

    public void getAutoFollowPattern(
        final GetAutoFollowPatternAction.Request request,
        final ActionListener<GetAutoFollowPatternAction.Response> listener) {
        client.execute(GetAutoFollowPatternAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetAutoFollowPatternAction.Response> getAutoFollowPattern(final GetAutoFollowPatternAction.Request request) {
        final PlainActionFuture<GetAutoFollowPatternAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetAutoFollowPatternAction.INSTANCE, request, listener);
        return listener;
    }

}
