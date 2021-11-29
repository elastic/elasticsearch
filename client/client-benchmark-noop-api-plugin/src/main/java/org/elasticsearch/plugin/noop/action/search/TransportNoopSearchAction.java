/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugin.noop.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;

public class TransportNoopSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    @Inject
    public TransportNoopSearchAction(TransportService transportService, ActionFilters actionFilters) {
        super(NoopSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        listener.onResponse(
            new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                    InternalAggregations.EMPTY,
                    new Suggest(Collections.emptyList()),
                    new SearchProfileResults(Collections.emptyMap()),
                    false,
                    false,
                    1
                ),
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
