/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeAction;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeRequest;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeResponse;
import org.elasticsearch.xpack.core.search.action.OpenPointInTimeAction;
import org.elasticsearch.xpack.core.search.action.OpenPointInTimeRequest;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.function.Function;

import static org.elasticsearch.action.ActionListener.wrap;
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

    private void searchWithPIT(SearchRequest search, ActionListener<SearchResponse> listener) {
        // don't increase the keep alive
        search.source().pointInTimeBuilder(new PointInTimeBuilder(pitId));
        // get the pid on each response
        super.search(search, pitListener(SearchResponse::pointInTimeId, listener));
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
        // don't increase the keep alive
        for (SearchRequest request : search.requests()) {
            request.source().pointInTimeBuilder(new PointInTimeBuilder(pitId));
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

    // listener handing the extraction of new PIT and closing in case of exceptions
    private <Response> ActionListener<Response> pitListener(Function<Response, String> pitIdExtractor, ActionListener<Response> listener) {
        return wrap(r -> {
                // get pid
                pitId = pitIdExtractor.apply(r);
                listener.onResponse(r);
            },
            // always close PIT in case of exceptions
            e -> {
                if (pitId != null) {
                    close(wrap(b -> {
                    }, listener::onFailure));
                }
                listener.onFailure(e);
            });
    }

    private <Response> void openPIT(ActionListener<Response> listener, Runnable runnable) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(
            indices,
            IndexResolver.FIELD_CAPS_INDICES_OPTIONS,
            keepAlive,
            null,
            null
        );
        client.execute(OpenPointInTimeAction.INSTANCE, request, wrap(r -> {
                pitId = r.getSearchContextId();
                runnable.run();
            },
            listener::onFailure));
    }

    @Override
    public void close(ActionListener<Boolean> listener) {
        client.execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitId),
            map(listener, ClosePointInTimeResponse::isSucceeded));
        pitId = null;
    }
}
