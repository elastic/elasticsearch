/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.idp.action.SamlGenerateMetadataAction;
import org.elasticsearch.xpack.idp.action.SamlGenerateMetadataRequest;
import org.elasticsearch.xpack.idp.action.SamlGenerateMetadataResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSamlGenerateMetadataRequest extends BaseRestHandler {

    static final ObjectParser<SamlGenerateMetadataRequest, Void> PARSER =
        new ObjectParser<>("idp_generate_metadata", SamlGenerateMetadataRequest::new);

    static {
        PARSER.declareString(SamlGenerateMetadataRequest::setSpEntityId, new ParseField("sp_entity_id"));
    }

    @Override
    public String getName() {
        return "saml_idp_generate_metadata";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(GET, "/_idp/saml/metadata"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlGenerateMetadataRequest generateMetadataRequest = PARSER.parse(parser, null);
            return channel -> client.execute(SamlGenerateMetadataAction.INSTANCE, generateMetadataRequest,
                new RestBuilderListener<SamlGenerateMetadataResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlGenerateMetadataResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("metadata", response.getXmlString());
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }
}
