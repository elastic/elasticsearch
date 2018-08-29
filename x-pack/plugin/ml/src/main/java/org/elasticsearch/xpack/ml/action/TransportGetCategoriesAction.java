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
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetCategoriesAction extends HandledTransportAction<GetCategoriesAction.Request, GetCategoriesAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final Client client;
    private final JobManager jobManager;

    @Inject
    public TransportGetCategoriesAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        JobResultsProvider jobResultsProvider, Client client, JobManager jobManager) {
        super(settings, GetCategoriesAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetCategoriesAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(GetCategoriesAction.Request request, ActionListener<GetCategoriesAction.Response> listener) {
        jobManager.getJob(request.getJobId(), ActionListener.wrap(
                job -> {
                    Integer from = request.getPageParams() != null ? request.getPageParams().getFrom() : null;
                    Integer size = request.getPageParams() != null ? request.getPageParams().getSize() : null;
                    jobResultsProvider.categoryDefinitions(request.getJobId(), request.getCategoryId(), true, from, size,
                            r -> listener.onResponse(new GetCategoriesAction.Response(r)), listener::onFailure, client);
                },
                listener::onFailure
        ));
    }
}
