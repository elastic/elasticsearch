/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.TransportRelayAction;
import org.elasticsearch.xpack.core.security.action.TransportRelayRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class RestTransportRelayAction extends SecurityBaseRestHandler {

    static final ConstructingObjectParser<TransportRelayRequest, Void> PARSER = new ConstructingObjectParser<>(
        "transport_relay_request",
        a -> new TransportRelayRequest((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("action"));
        PARSER.declareString(constructorArg(), new ParseField("payload"));
    }

    public RestTransportRelayAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "transport_relay_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_transport_relay"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final TransportRelayRequest transportRelayRequest = PARSER.parse(parser, null);
            return channel -> client.execute(TransportRelayAction.INSTANCE, transportRelayRequest, new RestToXContentListener<>(channel));
        }
    }
}
