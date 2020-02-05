/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestWatchServiceAction extends BaseRestHandler {

    @Override
    public Map<String, List<Method>> handledMethodsAndPaths() {
        return Collections.singletonMap("/_watcher/_start", singletonList(POST));
    }

    @Override
    public String getName() {
        return "watcher_start_service";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel ->
            client.execute(WatcherServiceAction.INSTANCE, new WatcherServiceRequest().start(), new RestToXContentListener<>(channel));
    }

    public static class StopRestHandler extends BaseRestHandler {

        @Override
        public Map<String, List<Method>> handledMethodsAndPaths() {
            return Collections.singletonMap("/_watcher/_stop", singletonList(POST));
        }

        @Override
        public String getName() {
            return "watcher_stop_service";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            return channel ->
                client.execute(WatcherServiceAction.INSTANCE, new WatcherServiceRequest().stop(), new RestToXContentListener<>(channel));
        }
    }
}
