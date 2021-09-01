/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING;

public class FrozenCacheInfoNodeAction extends ActionType<FrozenCacheInfoResponse> {

    public static final String NAME = FrozenCacheInfoAction.NAME + "[n]";
    public static final FrozenCacheInfoNodeAction INSTANCE = new FrozenCacheInfoNodeAction();

    private FrozenCacheInfoNodeAction() {
        super(NAME, FrozenCacheInfoResponse::new);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, FrozenCacheInfoResponse> {

        private final FrozenCacheInfoResponse response;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ActionFilters actionFilters) {
            super(NAME, transportService, actionFilters, Request::new);
            response = new FrozenCacheInfoResponse(SNAPSHOT_CACHE_SIZE_SETTING.get(settings).isNonZeroSize());
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<FrozenCacheInfoResponse> listener) {
            listener.onResponse(response);
        }

    }

}
