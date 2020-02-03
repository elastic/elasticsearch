/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnRequest;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSamlInitiateSingleSignOnAction extends BaseRestHandler {
    static final ObjectParser<SamlInitiateSingleSignOnRequest, Void> PARSER = new ObjectParser<>("idp_init_sso",
        SamlInitiateSingleSignOnRequest::new);

    static {
        PARSER.declareString(SamlInitiateSingleSignOnRequest::setSpEntityId, new ParseField("sp_entity_id"));
    }

    public RestSamlInitiateSingleSignOnAction(RestController controller) {
        controller.registerHandler(
            POST, "/_idp/saml/init", this
        );
    }

    @Override
    public String getName() {
        return "idp_init_sso_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlInitiateSingleSignOnRequest initRequest = PARSER.parse(parser, null);
            return channel -> client.execute(SamlInitiateSingleSignOnAction.INSTANCE, initRequest,
                new RestBuilderListener<SamlInitiateSingleSignOnResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlInitiateSingleSignOnResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("redirect_url", response.getRedirectUrl());
                        builder.field("response_body", response.getResponseBody());
                        builder.field("sp_entity_id", response.getSpEntityId());
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }
}
