/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.ScheduledEventsQueryBuilder;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class TransportGetCalendarEventsAction extends HandledTransportAction<GetCalendarEventsAction.Request,
        GetCalendarEventsAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final ClusterService clusterService;

    @Inject
    public TransportGetCalendarEventsAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService,
                                            JobResultsProvider jobResultsProvider) {
        super(GetCalendarEventsAction.NAME, transportService, actionFilters,
            (Supplier<GetCalendarEventsAction.Request>) GetCalendarEventsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetCalendarEventsAction.Request request,
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
                        ClusterState state = clusterService.state();
                        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(state);

                        List<String> jobGroups;
                        String requestId = request.getJobId();

                        Job job = currentMlMetadata.getJobs().get(request.getJobId());
                        if (job == null) {
                            // Check if the requested id is a job group
                            if (currentMlMetadata.isGroupOrJob(request.getJobId()) == false) {
                                listener.onFailure(ExceptionsHelper.missingJobException(request.getJobId()));
                                return;
                            }
                            jobGroups = Collections.singletonList(request.getJobId());
                            requestId = null;
                        } else {
                            jobGroups = job.getGroups();
                        }

                        jobResultsProvider.scheduledEventsForJob(requestId, jobGroups, query, eventsListener);
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
