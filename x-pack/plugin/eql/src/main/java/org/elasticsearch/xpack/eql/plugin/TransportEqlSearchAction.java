/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse> {
    private final SecurityContext securityContext;
    private final ClusterService clusterService;

    @Inject
    public TransportEqlSearchAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                   ThreadPool threadPool, ActionFilters actionFilters) {
        super(EqlSearchAction.NAME, transportService, actionFilters, EqlSearchRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        operation(request, listener);
    }

    public static void operation(EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        // TODO: implement parsing and querying
        listener.onResponse(createResponse(request));
    }

    static EqlSearchResponse createResponse(EqlSearchRequest request) {
        // Stubbed search response
        // TODO: implement actual search response processing once the parser/executor is in place
        List<SearchHit> events = Arrays.asList(
            new SearchHit(1, "111", null),
            new SearchHit(2, "222", null)
        );
        EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(null, Arrays.asList(
            new EqlSearchResponse.Sequence(Collections.singletonList("4021"), events),
            new EqlSearchResponse.Sequence(Collections.singletonList("2343"), events)
        ), null, new TotalHits(0, TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 0, false);
    }
}
