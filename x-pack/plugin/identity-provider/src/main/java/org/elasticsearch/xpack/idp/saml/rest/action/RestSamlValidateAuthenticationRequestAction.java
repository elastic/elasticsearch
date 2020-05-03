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
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestRequest;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSamlValidateAuthenticationRequestAction extends IdpBaseRestHandler {

    static final ObjectParser<SamlValidateAuthnRequestRequest, Void> PARSER =
        new ObjectParser<>("idp_validate_authn_request", SamlValidateAuthnRequestRequest::new);

    static {
        PARSER.declareString(SamlValidateAuthnRequestRequest::setQueryString, new ParseField("authn_request_query"));
    }

    public RestSamlValidateAuthenticationRequestAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public String getName() {
        return "saml_idp_validate_authn_request_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, "/_idp/saml/validate"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlValidateAuthnRequestRequest validateRequest = PARSER.parse(parser, null);
            return channel -> client.execute(SamlValidateAuthnRequestAction.INSTANCE, validateRequest,
                new RestBuilderListener<SamlValidateAuthnRequestResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlValidateAuthnRequestResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.startObject("service_provider");
                        builder.field("entity_id", response.getSpEntityId());
                        builder.field("acs", response.getAssertionConsumerService());
                        builder.endObject();
                        builder.field("force_authn", response.isForceAuthn());
                        builder.field("authn_state", response.getAuthnState());
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }
}
