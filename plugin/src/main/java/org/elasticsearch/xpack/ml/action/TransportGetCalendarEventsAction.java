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
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.SpecialEventsQueryBuilder;

import java.util.Collections;

public class TransportGetCalendarEventsAction extends HandledTransportAction<GetCalendarEventsAction.Request,
        GetCalendarEventsAction.Response> {

    private final JobProvider jobProvider;

    @Inject
    public TransportGetCalendarEventsAction(Settings settings, ThreadPool threadPool,
                                            TransportService transportService, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            JobProvider jobProvider) {
        super(settings, GetCalendarEventsAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, GetCalendarEventsAction.Request::new);
        this.jobProvider = jobProvider;
    }

    @Override
    protected void doExecute(GetCalendarEventsAction.Request request,
                             ActionListener<GetCalendarEventsAction.Response> listener) {
        ActionListener<Boolean> calendarExistsListener = ActionListener.wrap(
                r -> {
                    SpecialEventsQueryBuilder query = new SpecialEventsQueryBuilder()
                            .after(request.getAfter())
                            .before(request.getBefore())
                            .from(request.getPageParams().getFrom())
                            .size(request.getPageParams().getSize());

                    if (GetCalendarsAction.Request.ALL.equals(request.getCalendarId()) == false) {
                        query.calendarIds(Collections.singletonList(request.getCalendarId()));
                    }

                    ActionListener<QueryPage<SpecialEvent>> eventsListener = ActionListener.wrap(
                            events -> {
                                listener.onResponse(new GetCalendarEventsAction.Response(events));
                            },
                            listener::onFailure
                    );

                    if (request.getJobId() != null) {
                        jobProvider.specialEventsForJob(request.getJobId(), query, eventsListener);
                    } else {
                        jobProvider.specialEvents(query, eventsListener);
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

        jobProvider.calendar(calendarId, ActionListener.wrap(
                c -> listener.onResponse(true),
                listener::onFailure
        ));
    }
}
