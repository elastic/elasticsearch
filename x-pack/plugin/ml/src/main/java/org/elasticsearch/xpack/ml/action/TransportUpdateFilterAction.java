/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.job.JobManager;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportUpdateFilterAction extends HandledTransportAction<UpdateFilterAction.Request, PutFilterAction.Response> {

    private final Client client;
    private final JobManager jobManager;

    @Inject
    public TransportUpdateFilterAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        JobManager jobManager
    ) {
        super(
            UpdateFilterAction.NAME,
            transportService,
            actionFilters,
            UpdateFilterAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(Task task, UpdateFilterAction.Request request, ActionListener<PutFilterAction.Response> listener) {
        ActionListener<FilterWithSeqNo> filterListener = listener.delegateFailureAndWrap(
            (l, filterWithVersion) -> updateFilter(filterWithVersion, request, l)
        );

        getFilterWithVersion(request.getFilterId(), filterListener);
    }

    private void updateFilter(
        FilterWithSeqNo filterWithVersion,
        UpdateFilterAction.Request request,
        ActionListener<PutFilterAction.Response> listener
    ) {
        MlFilter filter = filterWithVersion.filter;

        if (request.isNoop()) {
            listener.onResponse(new PutFilterAction.Response(filter));
            return;
        }

        String description = request.getDescription() == null ? filter.getDescription() : request.getDescription();
        SortedSet<String> items = new TreeSet<>(filter.getItems());
        items.addAll(request.getAddItems());

        // Check if removed items are present to avoid typos
        for (String toRemove : request.getRemoveItems()) {
            boolean wasPresent = items.remove(toRemove);
            if (wasPresent == false) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Cannot remove item [" + toRemove + "] as it is not present in filter [" + filter.getId() + "]"
                    )
                );
                return;
            }
        }

        MlFilter updatedFilter = MlFilter.builder(filter.getId()).setDescription(description).setItems(items).build();
        indexUpdatedFilter(updatedFilter, filterWithVersion.seqNo, filterWithVersion.primaryTerm, request, listener);
    }

    private void indexUpdatedFilter(
        MlFilter filter,
        final long seqNo,
        final long primaryTerm,
        UpdateFilterAction.Request request,
        ActionListener<PutFilterAction.Response> listener
    ) {
        IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName()).id(filter.documentId());
        indexRequest.setIfSeqNo(seqNo);
        indexRequest.setIfPrimaryTerm(primaryTerm);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
            indexRequest.source(filter.toXContent(builder, params));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialise filter with id [" + filter.getId() + "]", e);
        }

        executeAsyncWithOrigin(client, ML_ORIGIN, TransportIndexAction.TYPE, indexRequest, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse indexResponse) {
                jobManager.notifyFilterChanged(
                    filter,
                    request.getAddItems(),
                    request.getRemoveItems(),
                    listener.delegateFailureAndWrap((l, response) -> l.onResponse(new PutFilterAction.Response(filter)))
                );
            }

            @Override
            public void onFailure(Exception e) {
                Exception reportedException;
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    reportedException = ExceptionsHelper.conflictStatusException(
                        "Error updating filter with id [" + filter.getId() + "] because it was modified while the update was in progress",
                        e
                    );
                } else {
                    reportedException = ExceptionsHelper.serverError("Error updating filter with id [" + filter.getId() + "]", e);
                }
                listener.onFailure(reportedException);
            }
        });
    }

    private void getFilterWithVersion(String filterId, ActionListener<FilterWithSeqNo> listener) {
        GetRequest getRequest = new GetRequest(MlMetaIndex.indexName(), MlFilter.documentId(filterId));
        executeAsyncWithOrigin(client, ML_ORIGIN, TransportGetAction.TYPE, getRequest, listener.delegateFailure((l, getDocResponse) -> {
            try {
                if (getDocResponse.isExists()) {
                    try (
                        XContentParser parser = XContentHelper.createParserNotCompressed(
                            LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                            getDocResponse.getSourceAsBytesRef(),
                            XContentType.JSON
                        )
                    ) {
                        MlFilter filter = MlFilter.LENIENT_PARSER.apply(parser, null).build();
                        l.onResponse(new FilterWithSeqNo(filter, getDocResponse));
                    }
                } else {
                    l.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.FILTER_NOT_FOUND, filterId)));
                }
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }

    private static class FilterWithSeqNo {

        private final MlFilter filter;
        private final long seqNo;
        private final long primaryTerm;

        private FilterWithSeqNo(MlFilter filter, GetResponse getDocResponse) {
            this.filter = filter;
            this.seqNo = getDocResponse.getSeqNo();
            this.primaryTerm = getDocResponse.getPrimaryTerm();

        }
    }
}
