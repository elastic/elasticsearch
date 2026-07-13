/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.idp.action.SamlMetadataAction;
import org.elasticsearch.xpack.idp.action.SamlMetadataRequest;
import org.elasticsearch.xpack.idp.action.SamlMetadataResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSamlMetadataAction extends IdpBaseRestHandler {

    public RestSamlMetadataAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public String getName() {
        return "saml_metadata_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_idp/saml/metadata/{sp_entity_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String spEntityId = request.param("sp_entity_id");
        final String acs = request.param("acs");
        final SamlMetadataRequest metadataRequest = new SamlMetadataRequest(spEntityId, acs);
        return channel -> client.execute(
            SamlMetadataAction.INSTANCE,
            metadataRequest,
            new RestBuilderListener<SamlMetadataResponse>(channel) {
                @Override
                public RestResponse buildResponse(SamlMetadataResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("metadata", response.getXmlString());
                    builder.endObject();
                    return new RestResponse(RestStatus.OK, builder);
                }
            }
        );

    }
}
