/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.ScheduledEventsQueryBuilder;

import java.util.Collections;

public class TransportGetCalendarEventsAction extends HandledTransportAction<GetCalendarEventsAction.Request,
        GetCalendarEventsAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;

    @Inject
    public TransportGetCalendarEventsAction(Settings settings, ThreadPool threadPool,
                                            TransportService transportService, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            JobResultsProvider jobResultsProvider, JobManager jobManager) {
        super(settings, GetCalendarEventsAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, GetCalendarEventsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(GetCalendarEventsAction.Request request,
                             ActionListener<GetCalendarEventsAction.Response> listener) {
        ActionListener<Boolean> calendarExistsListener = ActionListener.wrap(
                r -> {
                    ScheduledEventsQueryBuilder query = new ScheduledEventsQueryBuilder()
                            .start(request.getStart())
                            .end(request.getEnd())
                            .from(request.getPageParams().getFrom())
                            .size(request.getPageParams().getSize());

                    if (GetCalendarsAction.Request.ALL.equals(request.getCalendarId()) == false) {
                        query.calendarIds(Collections.singletonList(request.getCalendarId()));
                    }

                    ActionListener<QueryPage<ScheduledEvent>> eventsListener = ActionListener.wrap(
                            events -> {
                                listener.onResponse(new GetCalendarEventsAction.Response(events));
                            },
                            listener::onFailure
                    );

                    if (request.getJobId() != null) {

                        jobManager.getJob(request.getJobId(), ActionListener.wrap(
                                job -> {
                                    jobResultsProvider.scheduledEventsForJob(request.getJobId(), job.getGroups(), query, eventsListener);
                                },
                                jobNotFound -> {
                                    // is the request Id a group?
                                    jobManager.groupExists(request.getJobId(), ActionListener.wrap(
                                            groupExists -> {
                                                if (groupExists) {
                                                    jobResultsProvider.scheduledEventsForJob(
                                                            null, Collections.singletonList(request.getJobId()), query, eventsListener);
                                                } else {
                                                    listener.onFailure(ExceptionsHelper.missingJobException(request.getJobId()));
                                                }
                                            },
                                            listener::onFailure
                                    ));
                                }
                        ));
                    } else {
                        jobResultsProvider.scheduledEvents(query, eventsListener);
                    }
                },
                listener::onFailure);

        checkCalendarExists(request.getCalendarId(), calendarExistsListener);
    }

    private void checkCalendarExists(String calendarId, ActionListener<Boolean> listener) {
        if (GetCalendarsAction.Request.ALL.equals(calendarId)) {
            listener.onResponse(true);
            return;
        }

        jobResultsProvider.calendar(calendarId, ActionListener.wrap(
                c -> listener.onResponse(true),
                listener::onFailure
        ));
    }
}
