/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.DeleteCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteCCMConfigurationAction extends BaseRestHandler {

    private final CCMFeature ccmFeature;

    public RestDeleteCCMConfigurationAction(CCMFeature ccmFeature) {
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
    }

    @Override
    public String getName() {
        return "delete_inference_ccm_configuration_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, INFERENCE_CCM_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            throw CCM_FORBIDDEN_EXCEPTION;
        }

        return channel -> client.execute(
            DeleteCCMConfigurationAction.INSTANCE,
            new DeleteCCMConfigurationAction.Request(RestUtils.getMasterNodeTimeout(restRequest), RestUtils.getAckTimeout(restRequest)),
            new RestToXContentListener<>(channel)
        );
    }
}
