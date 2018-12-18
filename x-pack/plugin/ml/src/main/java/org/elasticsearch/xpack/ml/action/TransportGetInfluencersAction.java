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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetInfluencersAction extends HandledTransportAction<GetInfluencersAction.Request, GetInfluencersAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final Client client;
    private final JobManager jobManager;

    @Inject
    public TransportGetInfluencersAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         JobResultsProvider jobResultsProvider, Client client, JobManager jobManager) {
        super(settings, GetInfluencersAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetInfluencersAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(GetInfluencersAction.Request request, ActionListener<GetInfluencersAction.Response> listener) {

        jobManager.jobExists(request.getJobId(), ActionListener.wrap(
                jobFound -> {
                    InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder()
                            .includeInterim(request.isExcludeInterim() == false)
                            .start(request.getStart())
                            .end(request.getEnd())
                            .from(request.getPageParams().getFrom())
                            .size(request.getPageParams().getSize())
                            .influencerScoreThreshold(request.getInfluencerScore())
                            .sortField(request.getSort())
                            .sortDescending(request.isDescending()).build();
                    jobResultsProvider.influencers(request.getJobId(), query,
                            page -> listener.onResponse(new GetInfluencersAction.Response(page)), listener::onFailure, client);
                },
                listener::onFailure)
        );
    }
}
