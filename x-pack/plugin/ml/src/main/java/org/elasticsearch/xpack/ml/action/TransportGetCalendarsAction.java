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
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.job.persistence.CalendarQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Collections;

public class TransportGetCalendarsAction extends HandledTransportAction<GetCalendarsAction.Request, GetCalendarsAction.Response> {

    private final JobResultsProvider jobResultsProvider;

    @Inject
    public TransportGetCalendarsAction(Settings settings, ThreadPool threadPool,
                           TransportService transportService, ActionFilters actionFilters,
                           IndexNameExpressionResolver indexNameExpressionResolver,
                           JobResultsProvider jobResultsProvider) {
        super(settings, GetCalendarsAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, GetCalendarsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected void doExecute(GetCalendarsAction.Request request, ActionListener<GetCalendarsAction.Response> listener) {
        final String calendarId = request.getCalendarId();
        if (request.getCalendarId() != null && GetCalendarsAction.Request.ALL.equals(request.getCalendarId()) == false) {
            getCalendar(calendarId, listener);
        } else {
            PageParams pageParams = request.getPageParams();
            if (pageParams == null) {
                pageParams = PageParams.defaultParams();
            }
            getCalendars(pageParams, listener);
        }
    }

    private void getCalendar(String calendarId, ActionListener<GetCalendarsAction.Response> listener) {

        jobResultsProvider.calendar(calendarId, ActionListener.wrap(
                calendar -> {
                    QueryPage<Calendar> page = new QueryPage<>(Collections.singletonList(calendar), 1, Calendar.RESULTS_FIELD);
                    listener.onResponse(new GetCalendarsAction.Response(page));
                },
                listener::onFailure
        ));
    }

    private void getCalendars(PageParams pageParams, ActionListener<GetCalendarsAction.Response> listener) {
        CalendarQueryBuilder query = new CalendarQueryBuilder().pageParams(pageParams).sort(true);
        jobResultsProvider.calendars(query, ActionListener.wrap(
                calendars -> {
                    listener.onResponse(new GetCalendarsAction.Response(calendars));
                },
                listener::onFailure
        ));
    }
}
