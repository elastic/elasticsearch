/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.idp.action.DeleteSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.DeleteSamlServiceProviderRequest;
import org.elasticsearch.xpack.idp.action.DeleteSamlServiceProviderResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to remove a service provider (by Entity ID) from the IdP.
 */
public class RestDeleteSamlServiceProviderAction extends IdpBaseRestHandler {

    public RestDeleteSamlServiceProviderAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public String getName() {
        return "idp_delete_saml_sp_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_idp/saml/sp/{sp_entity_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String entityId = restRequest.param("sp_entity_id");
        final WriteRequest.RefreshPolicy refresh = restRequest.hasParam("refresh")
            ? WriteRequest.RefreshPolicy.parse(restRequest.param("refresh")) : WriteRequest.RefreshPolicy.NONE;
        final DeleteSamlServiceProviderRequest request = new DeleteSamlServiceProviderRequest(entityId, refresh);
        return channel -> client.execute(DeleteSamlServiceProviderAction.INSTANCE, request,
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeleteSamlServiceProviderResponse response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, restRequest);
                    return new BytesRestResponse(response.found() ? RestStatus.OK : RestStatus.NOT_FOUND, builder);
                }
            });
    }
}
