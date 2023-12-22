/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugin.noop;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.noop.action.bulk.RestNoopBulkAction;
import org.elasticsearch.plugin.noop.action.bulk.TransportNoopBulkAction;
import org.elasticsearch.plugin.noop.action.search.RestNoopSearchAction;
import org.elasticsearch.plugin.noop.action.search.TransportNoopSearchAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;

public class NoopPlugin extends Plugin implements ActionPlugin {

    public static final ActionType<SearchResponse> NOOP_SEARCH_ACTION = new ActionType<>("mock:data/read/search", SearchResponse::new);
    public static final ActionType<BulkResponse> NOOP_BULK_ACTION = new ActionType<>("mock:data/write/bulk", BulkResponse::new);

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(NOOP_BULK_ACTION, TransportNoopBulkAction.class),
            new ActionHandler<>(NOOP_SEARCH_ACTION, TransportNoopSearchAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return Arrays.asList(new RestNoopBulkAction(), new RestNoopSearchAction());
    }
}
