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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Set;

public class TransportUpdateCalendarJobAction extends HandledTransportAction<UpdateCalendarJobAction.Request, PutCalendarAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;

    @Inject
    public TransportUpdateCalendarJobAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        JobManager jobManager
    ) {
        super(
            UpdateCalendarJobAction.NAME,
            transportService,
            actionFilters,
            UpdateCalendarJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(Task task, UpdateCalendarJobAction.Request request, ActionListener<PutCalendarAction.Response> listener) {
        Set<String> jobIdsToAdd = Strings.tokenizeByCommaToSet(request.getJobIdsToAddExpression());
        Set<String> jobIdsToRemove = Strings.tokenizeByCommaToSet(request.getJobIdsToRemoveExpression());

        jobResultsProvider.updateCalendar(request.getCalendarId(), jobIdsToAdd, jobIdsToRemove, c -> {
            jobManager.updateProcessOnCalendarChanged(
                c.getJobIds(),
                ActionListener.wrap(r -> listener.onResponse(new PutCalendarAction.Response(c)), listener::onFailure)
            );
        }, listener::onFailure);
    }
}
