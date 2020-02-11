/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetBasicStatus extends XPackRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetBasicStatus.class));

    RestGetBasicStatus() {}

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(GET, "/_license/basic_status", GET, URI_BASE + "/license/basic_status", deprecationLogger));
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) {
        return channel -> client.licensing().prepareGetStartBasic().execute(new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_basic_status";
    }

}
