/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class TransportPutCalendarAction extends HandledTransportAction<PutCalendarAction.Request, PutCalendarAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportPutCalendarAction(Settings settings, ThreadPool threadPool,
                           TransportService transportService, ActionFilters actionFilters,
                           IndexNameExpressionResolver indexNameExpressionResolver,
                           Client client, ClusterService clusterService) {
        super(settings, PutCalendarAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, PutCalendarAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(PutCalendarAction.Request request, ActionListener<PutCalendarAction.Response> listener) {
        Calendar calendar = request.getCalendar();

        checkJobsExist(calendar.getJobIds(), listener::onFailure);

        IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, calendar.documentId());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            indexRequest.source(calendar.toXContent(builder,
                    new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY, "true"))));
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
                        listener.onFailure(
                                ExceptionsHelper.serverError("Error putting calendar with id [" + calendar.getId() + "]", e));
                    }
                });
    }

    private void checkJobsExist(List<String> jobIds, Consumer<Exception> errorHandler) {
        ClusterState state = clusterService.state();
        MlMetadata mlMetadata = state.getMetaData().custom(MLMetadataField.TYPE);
        for (String jobId: jobIds) {
            Set<String> jobs = mlMetadata.expandJobIds(jobId, true);
            if (jobs.isEmpty()) {
                errorHandler.accept(ExceptionsHelper.missingJobException(jobId));
                return;
            }
        }
    }
}
