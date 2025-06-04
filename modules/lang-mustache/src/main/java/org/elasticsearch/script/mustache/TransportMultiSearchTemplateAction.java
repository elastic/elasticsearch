/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.script.mustache.TransportSearchTemplateAction.convert;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMultiSearchTemplateAction.class);

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;
    private final NodeClient client;
    private final SearchUsageHolder searchUsageHolder;

    @Inject
    public TransportMultiSearchTemplateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client,
        UsageService usageService,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        super(
            MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION.name(),
            transportService,
            actionFilters,
            MultiSearchTemplateRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.client = client;
        this.searchUsageHolder = usageService.getSearchUsageHolder();
    }

    @Override
    protected void doExecute(Task task, MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        List<Integer> originalSlots = new ArrayList<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(request.indicesOptions());
        if (request.maxConcurrentSearchRequests() != 0) {
            multiSearchRequest.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[request.requests().size()];
        for (int i = 0; i < items.length; i++) {
            SearchTemplateRequest searchTemplateRequest = request.requests().get(i);
            SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
            SearchRequest searchRequest;
            try {
                searchRequest = convert(
                    searchTemplateRequest,
                    searchTemplateResponse,
                    scriptService,
                    xContentRegistry,
                    clusterSupportsFeature,
                    searchUsageHolder
                );
            } catch (Exception e) {
                searchTemplateResponse.decRef();
                items[i] = new MultiSearchTemplateResponse.Item(null, e);
                if (ExceptionsHelper.status(e).getStatus() >= 500 && ExceptionsHelper.isNodeOrShardUnavailableTypeException(e) == false) {
                    logger.warn("MultiSearchTemplate convert failure", e);
                }
                continue;
            }
            items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            if (searchRequest != null) {
                multiSearchRequest.add(searchRequest);
                originalSlots.add(i);
            }
        }

        client.multiSearch(multiSearchRequest, listener.delegateFailureAndWrap((l, r) -> {
            for (int i = 0; i < r.getResponses().length; i++) {
                MultiSearchResponse.Item item = r.getResponses()[i];
                int originalSlot = originalSlots.get(i);
                if (item.isFailure()) {
                    var existing = items[originalSlot];
                    if (existing.getResponse() != null) {
                        existing.getResponse().decRef();
                    }
                    items[originalSlot] = new MultiSearchTemplateResponse.Item(null, item.getFailure());
                } else {
                    items[originalSlot].getResponse().setResponse(item.getResponse());
                    item.getResponse().incRef();
                }
            }
            ActionListener.respondAndRelease(l, new MultiSearchTemplateResponse(items, r.getTook().millis()));
        }));
    }
}
