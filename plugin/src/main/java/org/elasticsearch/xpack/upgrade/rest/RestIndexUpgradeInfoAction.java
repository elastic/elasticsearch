/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RestIndexUpgradeInfoAction extends BaseRestHandler {
    private final Set<String> extraParameters;

    public RestIndexUpgradeInfoAction(Settings settings, RestController controller, Set<String> extraParameters) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/_xpack/migration/assistance", this);
        controller.registerHandler(RestRequest.Method.GET, "/_xpack/migration/assistance/{index}", this);
        this.extraParameters = extraParameters;
    }


    @Override
    public String getName() {
        return "xpack_migration_assistance";
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
        infoRequest.indicesOptions(IndicesOptions.fromRequest(request, infoRequest.indicesOptions()));
        Map<String, String> extraParamsMap = new HashMap<>();
        for (String param : extraParameters) {
            String value = request.param(param);
            if (value != null) {
                extraParamsMap.put(param, value);
            }
        }
        infoRequest.extraParams(extraParamsMap);
        return channel -> client.execute(IndexUpgradeInfoAction.INSTANCE, infoRequest, new RestToXContentListener<>(channel));
    }

}

