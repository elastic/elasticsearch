/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch;

import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class XSearchAnalyticsService {
    
    private final ClusterService clusterService;

    public XSearchAnalyticsService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void recordEvent(XSearchAction.Request request, NodeClient client) {
        Collection<SearchEngine> engines = getEnginesForRequest(request);
        Collection<String> dataStreams = getDataStreamsForEngines(engines);
        dataStreams.forEach(ds -> recordEvent(ds, request, client));
    }

    private void recordEvent(String dataStream, XSearchAction.Request request, NodeClient client) {
        Map<String, Object> analyticsEvent = Map.of("query", request.getQuery());
        IndexRequest indexRequest = client.prepareIndex(dataStream).setSource(analyticsEvent).request();
        client.execute(IndexAction.INSTANCE, indexRequest);
    }

    private Collection<String> getDataStreamsForEngines(Collection<SearchEngine> engines) {
        Map<String, DataStream> dataStreams = clusterService.state().metadata().dataStreams();
        return engines.stream()
            .filter(SearchEngine::shouldRecordAnalytics)
            .map(SearchEngine::getAnalyticsCollection)
            .filter(dataStreams::containsKey)
            .toList();
    }

    private Collection<SearchEngine> getEnginesForRequest(XSearchAction.Request request) {
        Map<String, SearchEngine> engines = clusterService.state().metadata().searchEngines();
        return Arrays.stream(request.indices()).filter(engines::containsKey).map(engines::get).toList();
    }

}
