/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteCalendarAction extends HandledTransportAction<DeleteCalendarAction.Request, DeleteCalendarAction.Response> {

    private final Client client;

    @Inject
    public TransportDeleteCalendarAction(Settings settings, ThreadPool threadPool,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client) {
        super(settings, DeleteCalendarAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, DeleteCalendarAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(DeleteCalendarAction.Request request, ActionListener<DeleteCalendarAction.Response> listener) {

        final String calendarId = request.getCalendarId();

        GetRequest getRequest = new GetRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, Calendar.documentId(calendarId));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        if (getResponse.isExists() == false) {
                            listener.onFailure(new ResourceNotFoundException("Could not delete calendar [" + calendarId
                                    + "] because it does not exist"));
                            return;
                        }

                        // Delete calendar and events
                        DeleteByQueryRequest dbqRequest = buildDeleteByQuery(calendarId);
                        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, dbqRequest, ActionListener.wrap(
                                response -> listener.onResponse(new DeleteCalendarAction.Response(true)),
                                listener::onFailure));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(ExceptionsHelper.serverError("Could not delete calendar [" + calendarId + "]", e));
                    }
                }
        );
    }

    private DeleteByQueryRequest buildDeleteByQuery(String calendarId) {
        SearchRequest searchRequest = new SearchRequest(MlMetaIndex.INDEX_NAME);
        // The DBQ request constructor wipes the search request source
        // so it has to be set after
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        request.setSlices(5);
        request.setRefresh(true);

        QueryBuilder query = QueryBuilders.termsQuery(Calendar.ID.getPreferredName(), calendarId);
        searchRequest.source(new SearchSourceBuilder().query(query));
        return request;
    }
}
