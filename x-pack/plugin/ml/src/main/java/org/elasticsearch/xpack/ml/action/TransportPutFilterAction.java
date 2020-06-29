/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportPutFilterAction extends HandledTransportAction<PutFilterAction.Request, PutFilterAction.Response> {

    private final Client client;

    @Inject
    public TransportPutFilterAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(PutFilterAction.NAME, transportService, actionFilters, PutFilterAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PutFilterAction.Request request, ActionListener<PutFilterAction.Response> listener) {
        MlFilter filter = request.getFilter();
        IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName()).id(filter.documentId());
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
            indexRequest.source(filter.toXContent(builder, params));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialise filter with id [" + filter.getId() + "]", e);
        }

        executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest,
                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        listener.onResponse(new PutFilterAction.Response(filter));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Exception reportedException;
                        if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                            reportedException = new ResourceAlreadyExistsException("A filter with id [" + filter.getId()
                                    + "] already exists");
                        } else {
                            reportedException = ExceptionsHelper.serverError("Error putting filter with id [" + filter.getId() + "]", e);
                        }
                        listener.onFailure(reportedException);
                    }
                });
    }
}
