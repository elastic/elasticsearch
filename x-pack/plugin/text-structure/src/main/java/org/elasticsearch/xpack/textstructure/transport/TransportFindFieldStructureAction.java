/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.core.textstructure.action.AbstractFindStructureRequest;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;
import org.elasticsearch.xpack.textstructure.structurefinder.TimeoutChecker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

public class TransportFindFieldStructureAction extends HandledTransportAction<FindFieldStructureAction.Request, FindStructureResponse> {

    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportFindFieldStructureAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool
    ) {
        super(
            FindFieldStructureAction.NAME,
            transportService,
            actionFilters,
            FindFieldStructureAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, FindFieldStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        client.prepareSearch(request.getIndex())
            .setSize(request.getLinesToSample())
            .setFetchSource(true)
            .setQuery(QueryBuilders.existsQuery(request.getField()))
            .setFetchSource(new String[] { request.getField() }, null)
            .execute(ActionListener.wrap(searchResponse -> {
                long hitCount = searchResponse.getHits().getHits().length;
                if (hitCount < AbstractFindStructureRequest.MIN_SAMPLE_LINE_COUNT) {
                    throw new IllegalArgumentException("Input contained too few lines [" + hitCount + "] to obtain a meaningful sample");
                }
                List<String> messages = getMessages(searchResponse, request.getField());
                // As determining the text structure might take a while, we run
                // in a different thread to avoid blocking the network thread.
                threadPool.executor(GENERIC).execute(() -> {
                    try {
                        listener.onResponse(buildTextStructureResponse(messages, request));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }, listener::onFailure));
    }

    private List<String> getMessages(SearchResponse searchResponse, String field) {
        return Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> MapHelper.dig(field, Objects.requireNonNull(hit.getSourceAsMap())).toString())
            .collect(Collectors.toList());
    }

    private FindStructureResponse buildTextStructureResponse(List<String> messages, FindFieldStructureAction.Request request)
        throws Exception {
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("structure analysis", request.getTimeout(), threadPool.scheduler())) {
            TextStructureFinder textStructureFinder = structureFinderManager.makeBestStructureFinder(
                new ArrayList<>(),
                messages,
                new TextStructureOverrides(request),
                timeoutChecker
            );

            return new FindStructureResponse(textStructureFinder.getStructure());
        }
    }
}
