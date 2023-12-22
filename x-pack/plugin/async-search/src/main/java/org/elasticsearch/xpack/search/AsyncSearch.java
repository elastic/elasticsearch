/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;

public final class AsyncSearch extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
            new ActionHandler<>(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
            new ActionHandler<>(GetAsyncStatusAction.INSTANCE, TransportGetAsyncStatusAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return Arrays.asList(
            new RestSubmitAsyncSearchAction(parameters.restController().getSearchUsageHolder()),
            new RestGetAsyncSearchAction(),
            new RestGetAsyncStatusAction(),
            new RestDeleteAsyncSearchAction()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING);
    }
}
