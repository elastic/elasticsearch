/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;

import java.util.function.Supplier;

public class TransportGetInfluencersAction extends HandledTransportAction<GetInfluencersAction.Request, GetInfluencersAction.Response> {

    private final JobProvider jobProvider;
    private final Client client;
    private final JobManager jobManager;

    @Inject
    public TransportGetInfluencersAction(Settings settings, TransportService transportService,
                                         ActionFilters actionFilters, JobProvider jobProvider, Client client, JobManager jobManager) {
        super(settings, GetInfluencersAction.NAME, transportService, actionFilters,
            (Supplier<GetInfluencersAction.Request>) GetInfluencersAction.Request::new);
        this.jobProvider = jobProvider;
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(GetInfluencersAction.Request request, ActionListener<GetInfluencersAction.Response> listener) {
        jobManager.getJobOrThrowIfUnknown(request.getJobId());

        InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder()
                .includeInterim(request.isExcludeInterim() == false)
                .start(request.getStart())
                .end(request.getEnd())
                .from(request.getPageParams().getFrom())
                .size(request.getPageParams().getSize())
                .influencerScoreThreshold(request.getInfluencerScore())
                .sortField(request.getSort())
                .sortDescending(request.isDescending()).build();
        jobProvider.influencers(request.getJobId(), query,
                page -> listener.onResponse(new GetInfluencersAction.Response(page)), listener::onFailure, client);
    }
}
