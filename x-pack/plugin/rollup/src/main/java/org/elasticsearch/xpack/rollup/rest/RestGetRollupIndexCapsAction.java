/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetRollupIndexCapsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
            new DeprecationLogger(LogManager.getLogger(RestGetRollupIndexCapsAction.class));

    static final ParseField INDEX = new ParseField("index");

    public RestGetRollupIndexCapsAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, "/{index}/_rollup/data", this,
                GET, "/{index}/_xpack/rollup/data", deprecationLogger);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String index = restRequest.param(INDEX.getPreferredName());
        IndicesOptions options = IndicesOptions.fromRequest(restRequest, IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED);
        GetRollupIndexCapsAction.Request request =
            new GetRollupIndexCapsAction.Request(Strings.splitStringByCommaToArray(index), options);
        return channel -> client.execute(GetRollupIndexCapsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_rollup_index_caps";
    }

}
