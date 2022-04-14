/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.deprecation.DeprecationInfoAction.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestDeprecationInfoAction extends BaseRestHandler {

    private static final Logger esLogger = LogManager.getLogger(RestDeprecationInfoAction.class);
    private static final org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger(RestDeprecationInfoAction.class);
    private static final org.apache.logging.log4j.Logger log4jLogger = org.apache.logging.log4j.LogManager.getLogger(
        RestDeprecationInfoAction.class
    );

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_migration/deprecations").replaces(GET, "/_xpack/migration/deprecations", RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/_migration/deprecations")
                .replaces(GET, "/{index}/_xpack/migration/deprecations", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "deprecation_info";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        esLogger.info("heee info es logger");
        slf4jLogger.info("heee info slf4jLogger");
        log4jLogger.info("heee info log4jLogger");
        if (request.method().equals(GET)) {
            return handleGet(request, client);
        } else {
            throw new IllegalArgumentException("illegal method [" + request.method() + "] for request [" + request.path() + "]");
        }
    }

    private RestChannelConsumer handleGet(final RestRequest request, NodeClient client) {
        Request infoRequest = new Request(Strings.splitStringByCommaToArray(request.param("index")));
        return channel -> client.execute(DeprecationInfoAction.INSTANCE, infoRequest, new RestToXContentListener<>(channel));
    }
}
