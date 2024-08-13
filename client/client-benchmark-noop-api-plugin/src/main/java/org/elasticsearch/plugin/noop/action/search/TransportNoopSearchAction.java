/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugin.noop.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugin.noop.NoopPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;

public class TransportNoopSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    @Inject
    public TransportNoopSearchAction(TransportService transportService, ActionFilters actionFilters) {
        super(
            NoopPlugin.NOOP_SEARCH_ACTION.name(),
            transportService,
            actionFilters,
            (Writeable.Reader<SearchRequest>) SearchRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        listener.onResponse(
            new SearchResponse(
                SearchHits.EMPTY_WITH_TOTAL_HITS,
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                false,
                false,
                new SearchProfileResults(Collections.emptyMap()),
                1,
                "",
                1,
                1,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            )
        );
    }
}
