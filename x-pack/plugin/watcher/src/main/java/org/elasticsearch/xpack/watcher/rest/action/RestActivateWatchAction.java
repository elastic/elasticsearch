/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * The rest action to de/activate a watch
 */
public class RestActivateWatchAction extends WatcherRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestActivateWatchAction.class));

    @Override
    public List<Route> routes() {
            return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return unmodifiableList(asList(
            new ReplacedRoute(POST, "/_watcher/watch/{id}/_activate", POST, URI_BASE + "/watcher/watch/{id}/_activate", deprecationLogger),
            new ReplacedRoute(PUT, "/_watcher/watch/{id}/_activate", PUT, URI_BASE + "/watcher/watch/{id}/_activate", deprecationLogger)));
    }

    @Override
    public String getName() {
        return "watcher_activate_watch";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
        String watchId = request.param("id");
        return channel ->
                client.activateWatch(new ActivateWatchRequest(watchId, true), new RestBuilderListener<ActivateWatchResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                        return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                .endObject());
                    }
                });
    }

    public static class DeactivateRestHandler extends WatcherRestHandler {

        @Override
        public List<Route> routes() {
            return emptyList();
        }

        @Override
        public List<ReplacedRoute> replacedRoutes() {
            return unmodifiableList(asList(
                new ReplacedRoute(
                    POST, "/_watcher/watch/{id}/_deactivate",
                    POST, URI_BASE + "/watcher/watch/{id}/_deactivate", deprecationLogger),
                new ReplacedRoute(
                    PUT, "/_watcher/watch/{id}/_deactivate",
                    PUT, URI_BASE + "/watcher/watch/{id}/_deactivate", deprecationLogger)));
        }

        @Override
        public String getName() {
            return "watcher_deactivate_watch";
        }

        @Override
        public RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) {
            String watchId = request.param("id");
            return channel ->
                    client.activateWatch(new ActivateWatchRequest(watchId, false), new RestBuilderListener<ActivateWatchResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(ActivateWatchResponse response, XContentBuilder builder) throws Exception {
                            return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                    .field(WatchField.STATUS.getPreferredName(), response.getStatus(), WatcherParams.HIDE_SECRETS)
                                    .endObject());
                        }
                    });
        }
    }

}
