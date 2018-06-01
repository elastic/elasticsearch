/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

import java.io.IOException;

public class RestExplainLifecycleAction extends BaseRestHandler {

    public RestExplainLifecycleAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "_" + IndexLifecycle.NAME + "/explain", this);
        controller.registerHandler(RestRequest.Method.GET, "{index}/_" + IndexLifecycle.NAME + "/explain", this);
    }

    @Override
    public String getName() {
        return "xpack_lifecycle_explain_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        ExplainLifecycleAction.Request explainLifecycleRequest = new ExplainLifecycleAction.Request();
        explainLifecycleRequest.indices(indexes);
        explainLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));

        return channel -> client.execute(ExplainLifecycleAction.INSTANCE, explainLifecycleRequest, new RestToXContentListener<>(channel));
    }
}