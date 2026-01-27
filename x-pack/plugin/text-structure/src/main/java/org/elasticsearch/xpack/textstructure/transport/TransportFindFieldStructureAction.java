/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.core.textstructure.action.AbstractFindStructureRequest;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

public class TransportFindFieldStructureAction extends HandledTransportAction<FindFieldStructureAction.Request, FindStructureResponse> {

    private final Client client;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    @Inject
    public TransportFindFieldStructureAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool
    ) {
        super(FindFieldStructureAction.NAME, transportService, actionFilters, FindFieldStructureAction.Request::new, threadPool.generic());
        this.client = client;
        this.transportService = transportService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, FindFieldStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        TaskId taskId = new TaskId(transportService.getLocalNode().getId(), task.getId());
        new ParentTaskAssigningClient(client, taskId).prepareSearch(request.getIndex())
            .setSize(request.getLinesToSample())
            .setFetchSource(true)
            .setQuery(QueryBuilders.existsQuery(request.getField()))
            .setFetchSource(new String[] { request.getField() }, null)
            .execute(listener.delegateFailureAndWrap((delegate, searchResponse) -> {
                var hitCount = searchResponse.getHits().getHits().length;
                if (hitCount < AbstractFindStructureRequest.MIN_SAMPLE_LINE_COUNT) {
                    delegate.onFailure(
                        new IllegalArgumentException("Input contained too few lines [" + hitCount + "] to obtain a meaningful sample")
                    );
                    return;
                }
                var messages = getMessages(searchResponse, request.getField());
                // As matching a regular expression might take a while, we run in a different thread to avoid blocking the network thread.
                threadPool.generic().execute(ActionRunnable.supply(delegate, () -> buildTextStructureResponse(messages, request)));
            }));
    }

    private List<String> getMessages(SearchResponse searchResponse, String field) {
        return Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> MapHelper.dig(field, Objects.requireNonNull(hit.getSourceAsMap())).toString())
            .collect(Collectors.toList());
    }

    private FindStructureResponse buildTextStructureResponse(List<String> messages, FindFieldStructureAction.Request request)
        throws Exception {
        assert ThreadPool.assertCurrentThreadPool(GENERIC);
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        TextStructureFinder textStructureFinder = structureFinderManager.findTextStructure(
            messages,
            new TextStructureOverrides(request),
            request.getTimeout()
        );
        return new FindStructureResponse(textStructureFinder.getStructure());
    }
}
