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
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.CalendarQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.ScheduledEventsQueryBuilder;

import java.util.Collections;

public class TransportGetCalendarEventsAction extends HandledTransportAction<
    GetCalendarEventsAction.Request,
    GetCalendarEventsAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final JobConfigProvider jobConfigProvider;

    @Inject
    public TransportGetCalendarEventsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        JobConfigProvider jobConfigProvider
    ) {
        super(
            GetCalendarEventsAction.NAME,
            transportService,
            actionFilters,
            GetCalendarEventsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.jobResultsProvider = jobResultsProvider;
        this.jobConfigProvider = jobConfigProvider;
    }

    @Override
    protected void doExecute(
        Task task,
        GetCalendarEventsAction.Request request,
        ActionListener<GetCalendarEventsAction.Response> listener
    ) {
        final String[] calendarId = Strings.splitStringByCommaToArray(request.getCalendarId());
        checkCalendarExists(calendarId, listener.delegateFailureAndWrap((outerDelegate, r) -> {
            ScheduledEventsQueryBuilder query = new ScheduledEventsQueryBuilder().start(request.getStart())
                .end(request.getEnd())
                .from(request.getPageParams().getFrom())
                .size(request.getPageParams().getSize())
                .calendarIds(calendarId);

            ActionListener<QueryPage<ScheduledEvent>> eventsListener = outerDelegate.delegateFailureAndWrap(
                (l, events) -> l.onResponse(new GetCalendarEventsAction.Response(events))
            );

            if (request.getJobId() != null) {

                jobConfigProvider.getJob(request.getJobId(), null, ActionListener.wrap(jobBuilder -> {
                    Job job = jobBuilder.build();
                    jobResultsProvider.scheduledEventsForJob(request.getJobId(), job.getGroups(), query, eventsListener);

                }, jobNotFound -> {
                    // is the request Id a group?
                    jobConfigProvider.groupExists(request.getJobId(), eventsListener.delegateFailureAndWrap((delegate, groupExists) -> {
                        if (groupExists) {
                            jobResultsProvider.scheduledEventsForJob(null, Collections.singletonList(request.getJobId()), query, delegate);
                        } else {
                            delegate.onFailure(ExceptionsHelper.missingJobException(request.getJobId()));
                        }
                    }));
                }));
            } else {
                jobResultsProvider.scheduledEvents(query, eventsListener);
            }
        }));
    }

    private void checkCalendarExists(String[] calendarId, ActionListener<Boolean> listener) {
        if (Strings.isAllOrWildcard(calendarId)) {
            listener.onResponse(true);
            return;
        }

        jobResultsProvider.calendars(
            CalendarQueryBuilder.builder().calendarIdTokens(calendarId),
            listener.delegateFailureAndWrap((l, c) -> l.onResponse(true))
        );
    }
}
