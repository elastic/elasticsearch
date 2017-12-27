/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class TransportPostCalendarEventsAction extends HandledTransportAction<PostCalendarEventsAction.Request,
        PostCalendarEventsAction.Response> {

    private final Client client;
    private final JobProvider jobProvider;

    @Inject
    public TransportPostCalendarEventsAction(Settings settings, ThreadPool threadPool,
                                             TransportService transportService, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Client client, JobProvider jobProvider) {
        super(settings, PostCalendarEventsAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, PostCalendarEventsAction.Request::new);
        this.client = client;
        this.jobProvider = jobProvider;
    }

    @Override
    protected void doExecute(PostCalendarEventsAction.Request request,
                             ActionListener<PostCalendarEventsAction.Response> listener) {
        List<SpecialEvent> events = request.getSpecialEvents();

        ActionListener<Boolean> calendarExistsListener = ActionListener.wrap(
                r -> {
                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                    for (SpecialEvent event: events) {
                        IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE);
                        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                            indexRequest.source(event.toXContent(builder,
                                    new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY,
                                            "true"))));
                        } catch (IOException e) {
                            throw new IllegalStateException("Failed to serialise special event", e);
                        }
                        bulkRequestBuilder.add(indexRequest);
                    }

                    bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                    executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkRequestBuilder.request(),
                            new ActionListener<BulkResponse>() {
                                @Override
                                public void onResponse(BulkResponse response) {
                                    listener.onResponse(new PostCalendarEventsAction.Response(events));
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(
                                            ExceptionsHelper.serverError("Error indexing special event", e));
                                }
                            });
                },
                listener::onFailure);

        checkCalendarExists(request.getCalendarId(), calendarExistsListener);
    }

    private void checkCalendarExists(String calendarId, ActionListener<Boolean> listener) {
        jobProvider.calendar(calendarId, ActionListener.wrap(
                c -> listener.onResponse(true),
                listener::onFailure
        ));
    }
}
