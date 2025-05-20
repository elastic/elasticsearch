/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetInfluencersAction extends HandledTransportAction<GetInfluencersAction.Request, GetInfluencersAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final Client client;
    private final JobManager jobManager;

    @Inject
    public TransportGetInfluencersAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        Client client,
        JobManager jobManager
    ) {
        super(
            GetInfluencersAction.NAME,
            transportService,
            actionFilters,
            GetInfluencersAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(Task task, GetInfluencersAction.Request request, ActionListener<GetInfluencersAction.Response> listener) {
        jobManager.jobExists(request.getJobId(), null, ActionListener.wrap(jobExists -> {
            InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder().includeInterim(
                request.isExcludeInterim() == false
            )
                .start(request.getStart())
                .end(request.getEnd())
                .from(request.getPageParams().getFrom())
                .size(request.getPageParams().getSize())
                .influencerScoreThreshold(request.getInfluencerScore())
                .sortField(request.getSort())
                .sortDescending(request.isDescending())
                .build();
            jobResultsProvider.influencers(
                request.getJobId(),
                query,
                page -> listener.onResponse(new GetInfluencersAction.Response(page)),
                listener::onFailure,
                client
            );
        }, listener::onFailure));
    }
}
