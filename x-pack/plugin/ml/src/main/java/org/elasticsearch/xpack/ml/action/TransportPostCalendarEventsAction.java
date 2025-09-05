/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportPostCalendarEventsAction extends HandledTransportAction<
    PostCalendarEventsAction.Request,
    PostCalendarEventsAction.Response> {

    private final Client client;
    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;

    @Inject
    public TransportPostCalendarEventsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        JobResultsProvider jobResultsProvider,
        JobManager jobManager
    ) {
        super(
            PostCalendarEventsAction.NAME,
            transportService,
            actionFilters,
            PostCalendarEventsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(
        Task task,
        PostCalendarEventsAction.Request request,
        ActionListener<PostCalendarEventsAction.Response> listener
    ) {
        List<ScheduledEvent> events = request.getScheduledEvents();

        ActionListener<Calendar> calendarListener = ActionListener.wrap(calendar -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            for (ScheduledEvent event : events) {
                IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName());
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    indexRequest.source(
                        event.toXContent(
                            builder,
                            new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"))
                        )
                    );
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to serialise event", e);
                }
                bulkRequestBuilder.add(indexRequest);
            }

            bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                TransportBulkAction.TYPE,
                bulkRequestBuilder.request(),
                new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        jobManager.updateProcessOnCalendarChanged(
                            calendar.getJobIds(),
                            ActionListener.wrap(
                                r -> listener.onResponse(new PostCalendarEventsAction.Response(events)),
                                listener::onFailure
                            )
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(ExceptionsHelper.serverError("Error indexing event", e));
                    }
                }
            );
        }, listener::onFailure);

        jobResultsProvider.calendar(request.getCalendarId(), calendarListener);
    }
}
