/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.idp.action.PutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.PutSamlServiceProviderRequest;
import org.elasticsearch.xpack.idp.action.PutSamlServiceProviderResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutSamlServiceProviderAction extends IdpBaseRestHandler {

    public RestPutSamlServiceProviderAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public String getName() {
        return "idp_put_saml_sp_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_idp/saml/sp/{sp_entity_id}"), new Route(POST, "/_idp/saml/sp/{sp_entity_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String entityId = restRequest.param("sp_entity_id");
        final WriteRequest.RefreshPolicy refreshPolicy = restRequest.hasParam("refresh")
            ? WriteRequest.RefreshPolicy.parse(restRequest.param("refresh"))
            : PutSamlServiceProviderRequest.DEFAULT_REFRESH_POLICY;
        try (XContentParser parser = restRequest.contentParser()) {
            final PutSamlServiceProviderRequest request = PutSamlServiceProviderRequest.fromXContent(entityId, refreshPolicy, parser);
            return channel -> client.execute(PutSamlServiceProviderAction.INSTANCE, request, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(PutSamlServiceProviderResponse response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, restRequest);
                    return new RestResponse(RestStatus.OK, builder);
                }
            });
        }
    }
}
