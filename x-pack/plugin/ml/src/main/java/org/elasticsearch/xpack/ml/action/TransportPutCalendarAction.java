/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportPutCalendarAction extends HandledTransportAction<PutCalendarAction.Request, PutCalendarAction.Response> {

    private final Client client;

    @Inject
    public TransportPutCalendarAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(PutCalendarAction.NAME, transportService, actionFilters, PutCalendarAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PutCalendarAction.Request request, ActionListener<PutCalendarAction.Response> listener) {
        Calendar calendar = request.getCalendar();

        IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName()).id(calendar.documentId());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            indexRequest.source(calendar.toXContent(builder,
                    new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"))));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialise calendar with id [" + calendar.getId() + "]", e);
        }

        // Make it an error to overwrite an existing calendar
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest,
                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        listener.onResponse(new PutCalendarAction.Response(calendar));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                            listener.onFailure(ExceptionsHelper.badRequestException("Cannot create calendar with id [" +
                                    calendar.getId() + "] as it already exists"));
                        } else {
                            listener.onFailure(
                                    ExceptionsHelper.serverError("Error putting calendar with id [" + calendar.getId() + "]", e));
                        }
                    }
                });
    }
}
