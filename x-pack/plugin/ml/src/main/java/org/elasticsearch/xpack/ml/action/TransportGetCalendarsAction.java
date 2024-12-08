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
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.ml.job.persistence.CalendarQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetCalendarsAction extends HandledTransportAction<GetCalendarsAction.Request, GetCalendarsAction.Response> {

    private final JobResultsProvider jobResultsProvider;

    @Inject
    public TransportGetCalendarsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider
    ) {
        super(
            GetCalendarsAction.NAME,
            transportService,
            actionFilters,
            GetCalendarsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected void doExecute(Task task, GetCalendarsAction.Request request, ActionListener<GetCalendarsAction.Response> listener) {
        final String[] calendarIds = Strings.splitStringByCommaToArray(request.getCalendarId());
        PageParams pageParams = request.getPageParams();
        if (pageParams == null) {
            pageParams = PageParams.defaultParams();
        }
        getCalendars(calendarIds, pageParams, listener);
    }

    private void getCalendars(String[] idTokens, PageParams pageParams, ActionListener<GetCalendarsAction.Response> listener) {
        CalendarQueryBuilder query = new CalendarQueryBuilder().pageParams(pageParams).calendarIdTokens(idTokens).sort(true);
        jobResultsProvider.calendars(
            query,
            ActionListener.wrap(calendars -> listener.onResponse(new GetCalendarsAction.Response(calendars)), listener::onFailure)
        );
    }
}
