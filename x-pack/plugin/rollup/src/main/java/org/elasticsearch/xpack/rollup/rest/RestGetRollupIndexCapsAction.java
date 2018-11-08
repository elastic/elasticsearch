/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

public class RestGetRollupIndexCapsAction extends BaseRestHandler {
    public static final ParseField INDEX = new ParseField("index");

    public RestGetRollupIndexCapsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_xpack/rollup/data", this);
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
        return "rollup_get_caps_action";
    }
}
