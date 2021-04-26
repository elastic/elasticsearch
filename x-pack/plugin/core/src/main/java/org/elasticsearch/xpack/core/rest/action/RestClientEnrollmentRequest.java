/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.action.ClientEnrollmentAction;
import org.elasticsearch.xpack.core.action.ClientEnrollmentRequest;
import org.elasticsearch.xpack.core.action.ClientEnrollmentResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RestClientEnrollmentRequest extends BaseRestHandler {

    static final ConstructingObjectParser<ClientEnrollmentRequest, Void> PARSER = new ConstructingObjectParser<>("enroll_client_request",
        a-> new ClientEnrollmentRequest((String) a[0], (SecureString) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("client_type"));
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), parser -> new SecureString(
                Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())),
            new ParseField("client_password"), ObjectParser.ValueType.STRING);
    }

    @Override public String getName() {
        return "client_enroll_action";
    }

    @Override public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_cluster/enroll_client"));
    }

    @Override protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            ClientEnrollmentRequest clientEnrollmentRequest = PARSER.apply(parser, null);
            return restChannel -> client.execute(ClientEnrollmentAction.INSTANCE, clientEnrollmentRequest,
                new RestBuilderListener<ClientEnrollmentResponse>(restChannel) {
                @Override public RestResponse buildResponse(
                    ClientEnrollmentResponse clientEnrollmentResponse, XContentBuilder builder) throws Exception {
                    clientEnrollmentResponse.toXContent(builder, channel.request());
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }

    }

}
