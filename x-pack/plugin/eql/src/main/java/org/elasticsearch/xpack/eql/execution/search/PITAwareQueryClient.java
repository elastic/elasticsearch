/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeAction;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeRequest;
import org.elasticsearch.xpack.core.search.action.ClosePointInTimeResponse;
import org.elasticsearch.xpack.core.search.action.OpenPointInTimeAction;
import org.elasticsearch.xpack.core.search.action.OpenPointInTimeRequest;
import org.elasticsearch.xpack.core.search.action.OpenPointInTimeResponse;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.ql.util.ActionListeners.map;

/**
 * Extension of basic query, adding Point-in-Time awareness.
 * Opens a point-in-time, uses it for all queries and closes it when disposed,
 * freeing consumer from doing any special management for it.
 */
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
            openPIT(wrap(r -> {
                pitId = r;
                searchWithPIT(search, listener);
            }, listener::onFailure));
        }
        else {
            searchWithPIT(search, listener);
        }
    }

    private void searchWithPIT(SearchRequest search, ActionListener<SearchResponse> listener) {
        // don't increase the keep alive
        search.source().pointInTimeBuilder(new PointInTimeBuilder(pitId));
        // get the pid on each request
        super.search(search, wrap(r -> {
                pitId = r.pointInTimeId();
                listener.onResponse(r);
            },
            // always close PIT in case of exceptions
            e -> {
                if (pitId != null) {
                    close(wrap(b -> {}, listener::onFailure));
                }
                listener.onFailure(e);
            }));
    }

    private void openPIT(ActionListener<String> listener) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(
            indices,
            IndexResolver.FIELD_CAPS_INDICES_OPTIONS,
            keepAlive,
            null,
            null
        );
        client.execute(OpenPointInTimeAction.INSTANCE, request, map(listener, OpenPointInTimeResponse::getSearchContextId));
    }

    @Override
    public void close(ActionListener<Boolean> listener)  {
        client.execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitId),
            map(listener, ClosePointInTimeResponse::isSucceeded));
        pitId = null;
    }
}
