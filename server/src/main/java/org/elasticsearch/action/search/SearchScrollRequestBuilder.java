/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;

/**
 * A search scroll action request builder.
 */
public class SearchScrollRequestBuilder extends ActionRequestBuilder<SearchScrollRequest, SearchResponse> {

    public SearchScrollRequestBuilder(ElasticsearchClient client, SearchScrollAction action) {
        super(client, action, new SearchScrollRequest());
    }

    public SearchScrollRequestBuilder(ElasticsearchClient client, SearchScrollAction action, String scrollId) {
        super(client, action, new SearchScrollRequest(scrollId));
    }

    /**
     * The scroll id to use to continue scrolling.
     */
    public SearchScrollRequestBuilder setScrollId(String scrollId) {
        request.scrollId(scrollId);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchScrollRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }
}
