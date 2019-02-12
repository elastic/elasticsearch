/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.rest;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndexUpgradeInfoAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestIndexUpgradeInfoAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestIndexUpgradeInfoAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerWithDeprecatedHandler(
            GET, "_migration/assistance", this,
            GET, "/_xpack/migration/assistance", deprecationLogger);

        controller.registerWithDeprecatedHandler(
            GET, "_migration/assistance/{index}", this,
            GET, "/_xpack/migration/assistance/{index}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "migration_assistance";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(GET)) {
            return handleGet(request, client);
        } else {
            throw new IllegalArgumentException("illegal method [" + request.method() + "] for request [" + request.path() + "]");
        }
    }

    private RestChannelConsumer handleGet(final RestRequest request, NodeClient client) {
        IndexUpgradeInfoRequest infoRequest = new IndexUpgradeInfoRequest(Strings.splitStringByCommaToArray(request.param("index")));
        infoRequest.indicesOptions(IndicesOptions.fromRequest(request, infoRequest.indicesOptions()));
        return channel -> client.execute(IndexUpgradeInfoAction.INSTANCE, infoRequest, new RestToXContentListener<>(channel));
    }

}

