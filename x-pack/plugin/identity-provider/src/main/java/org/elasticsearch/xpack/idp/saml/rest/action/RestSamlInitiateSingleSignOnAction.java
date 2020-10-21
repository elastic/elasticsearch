/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;

import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnRequest;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnResponse;
import org.elasticsearch.xpack.idp.saml.support.SamlAuthenticationState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSamlInitiateSingleSignOnAction extends IdpBaseRestHandler {
    static final ObjectParser<SamlInitiateSingleSignOnRequest, Void> PARSER = new ObjectParser<>("idp_init_sso",
        SamlInitiateSingleSignOnRequest::new);

    static {
        PARSER.declareString(SamlInitiateSingleSignOnRequest::setSpEntityId, new ParseField("entity_id"));
        PARSER.declareString(SamlInitiateSingleSignOnRequest::setAssertionConsumerService, new ParseField("acs"));
        PARSER.declareObject(SamlInitiateSingleSignOnRequest::setSamlAuthenticationState, (p, c) -> SamlAuthenticationState.fromXContent(p),
            new ParseField("authn_state"));
    }

    public RestSamlInitiateSingleSignOnAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, "/_idp/saml/init")
        );
    }

    @Override
    public String getName() {
        return "saml_idp_init_sso_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlInitiateSingleSignOnRequest initRequest = PARSER.parse(parser, null);
            return channel -> client.execute(SamlInitiateSingleSignOnAction.INSTANCE, initRequest,
                new RestBuilderListener<SamlInitiateSingleSignOnResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlInitiateSingleSignOnResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("post_url", response.getPostUrl());
                        builder.field("saml_response", response.getSamlResponse());
                        builder.field("saml_status", response.getSamlStatus());
                        builder.field("error", response.getError());
                        builder.startObject("service_provider");
                        builder.field("entity_id", response.getEntityId());
                        builder.endObject();
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }
}
