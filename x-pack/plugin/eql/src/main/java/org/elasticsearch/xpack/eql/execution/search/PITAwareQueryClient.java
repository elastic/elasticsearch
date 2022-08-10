/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.function.Function;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.ql.util.ActionListeners.map;

/**
 * Extension of basic query, adding Point-in-Time awareness.
 * Opens a point-in-time, uses it for all queries and closes it when disposed,
 * freeing consumer from doing any special management for it.
 */

// NB: cannot simplify the template further since client has different request/response types and different methods between
// search and multi-search hence the code repetition
public class PITAwareQueryClient extends BasicQueryClient {

    private String pitId;
    private final TimeValue keepAlive;

    public PITAwareQueryClient(EqlSession eqlSession) {
        super(eqlSession);
        this.keepAlive = eqlSession.configuration().requestTimeout();
    }

    @Override
    protected void search(SearchRequest search, ActionListener<SearchResponse> listener) {
        // no pitId, ask for one
        if (pitId == null) {
            openPIT(listener, () -> searchWithPIT(search, listener));
        } else {
            searchWithPIT(search, listener);
        }
    }

    private void searchWithPIT(SearchRequest request, ActionListener<SearchResponse> listener) {
        makeRequestPITCompatible(request);
        // get the pid on each response
        super.search(request, pitListener(SearchResponse::pointInTimeId, listener));
    }

    @Override
    protected void search(MultiSearchRequest search, ActionListener<MultiSearchResponse> listener) {
        // no pitId, ask for one
        if (pitId == null) {
            openPIT(listener, () -> searchWithPIT(search, listener));
        } else {
            searchWithPIT(search, listener);
        }
    }

    private void searchWithPIT(MultiSearchRequest search, ActionListener<MultiSearchResponse> listener) {
        for (SearchRequest request : search.requests()) {
            makeRequestPITCompatible(request);
        }

        // get the pid on each request
        super.search(search, pitListener(r -> {
            // get pid
            for (MultiSearchResponse.Item item : r.getResponses()) {
                // pick the first non-failing response
                if (item.isFailure() == false) {
                    return item.getResponse().pointInTimeId();
                }
            }
            // no results or successful responses, preserve the current pid
            return pitId;
        }, listener));
    }

    private void makeRequestPITCompatible(SearchRequest request) {
        SearchSourceBuilder source = request.source();
        // don't increase the keep alive
        source.pointInTimeBuilder(new PointInTimeBuilder(pitId));
        // move the indices from the search request to a index filter - see #63132
        String[] indices = request.indices();
        if (CollectionUtils.isEmpty(indices) == false) {
            request.indices(Strings.EMPTY_ARRAY);
            QueryBuilder indexQuery = indices.length == 1 ? termQuery(GetResult._INDEX, indices[0]) : termsQuery(GetResult._INDEX, indices);
            RuntimeUtils.addFilter(indexQuery, source);
        }
    }

    // listener handing the extraction of new PIT and closing in case of exceptions
    private <Response> ActionListener<Response> pitListener(Function<Response, String> pitIdExtractor, ActionListener<Response> listener) {
        return wrap(r -> {
            // get pid
            pitId = pitIdExtractor.apply(r);
            listener.onResponse(r);
        },
            // always close PIT in case of exceptions
            e -> {
                listener.onFailure(e);
                if (pitId != null && cfg.isCancelled() == false) {
                    // ignore any success/failure to avoid obfuscating the response
                    close(wrap(b -> {}, ex -> {}));
                }
            }
        );
    }

    private <Response> void openPIT(ActionListener<Response> listener, Runnable runnable) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).indicesOptions(IndexResolver.FIELD_CAPS_INDICES_OPTIONS)
            .keepAlive(keepAlive);
        client.execute(OpenPointInTimeAction.INSTANCE, request, wrap(r -> {
            pitId = r.getPointInTimeId();
            runnable.run();
        }, listener::onFailure));
    }

    @Override
    public void close(ActionListener<Boolean> listener) {
        // the pitId could be null as a consequence of a failure on openPIT
        if (pitId != null) {
            client.execute(
                ClosePointInTimeAction.INSTANCE,
                new ClosePointInTimeRequest(pitId),
                map(listener, ClosePointInTimeResponse::isSucceeded)
            );
            pitId = null;
        }
    }
}
