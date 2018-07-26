/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction.Request;

import java.io.IOException;

public class RestDeprecationInfoAction extends BaseRestHandler {
    public RestDeprecationInfoAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/_xpack/migration/deprecations", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_xpack/migration/deprecations", this);
    }

    @Override
    public String getName() {
        return "deprecation_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(RestRequest.Method.GET)) {
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
