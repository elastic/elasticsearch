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
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetRecordsAction extends HandledTransportAction<GetRecordsAction.Request, GetRecordsAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;
    private final Client client;

    @Inject
    public TransportGetRecordsAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     JobResultsProvider jobResultsProvider, JobManager jobManager, Client client) {
        super(settings, GetRecordsAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetRecordsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
        this.client = client;
    }

    @Override
    protected void doExecute(GetRecordsAction.Request request, ActionListener<GetRecordsAction.Response> listener) {

        jobManager.jobExists(request.getJobId(), ActionListener.wrap(
                jobFound -> {
                    RecordsQueryBuilder query = new RecordsQueryBuilder()
                            .includeInterim(request.isExcludeInterim() == false)
                            .epochStart(request.getStart())
                            .epochEnd(request.getEnd())
                            .from(request.getPageParams().getFrom())
                            .size(request.getPageParams().getSize())
                            .recordScore(request.getRecordScoreFilter())
                            .sortField(request.getSort())
                            .sortDescending(request.isDescending());
                    jobResultsProvider.records(request.getJobId(), query, page ->
                            listener.onResponse(new GetRecordsAction.Response(page)), listener::onFailure, client);
                },
                listener::onFailure
        ));
    }
}
