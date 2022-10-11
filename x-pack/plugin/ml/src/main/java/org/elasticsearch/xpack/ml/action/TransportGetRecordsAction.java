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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder;

public class TransportGetRecordsAction extends HandledTransportAction<GetRecordsAction.Request, GetRecordsAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;
    private final Client client;

    @Inject
    public TransportGetRecordsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        JobManager jobManager,
        Client client
    ) {
        super(GetRecordsAction.NAME, transportService, actionFilters, GetRecordsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetRecordsAction.Request request, ActionListener<GetRecordsAction.Response> listener) {

        jobManager.jobExists(request.getJobId(), null, ActionListener.wrap(jobExists -> {
            RecordsQueryBuilder query = new RecordsQueryBuilder().includeInterim(request.isExcludeInterim() == false)
                .epochStart(request.getStart())
                .epochEnd(request.getEnd())
                .from(request.getPageParams().getFrom())
                .size(request.getPageParams().getSize())
                .recordScore(request.getRecordScoreFilter())
                .sortField(request.getSort())
                .sortDescending(request.isDescending());
            jobResultsProvider.records(
                request.getJobId(),
                query,
                page -> listener.onResponse(new GetRecordsAction.Response(page)),
                listener::onFailure,
                client
            );
        }, listener::onFailure));
    }
}
