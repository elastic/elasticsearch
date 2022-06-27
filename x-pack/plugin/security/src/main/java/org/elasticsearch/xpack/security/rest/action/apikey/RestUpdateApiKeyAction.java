/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class RestUpdateApiKeyAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "update_api_key_request_payload",
        a -> new Payload((List<RoleDescriptor>) a[0], (Map<String, Object>) a[1])
    );

    static {
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return RoleDescriptor.parse(n, p, false);
        }, new ParseField("role_descriptors"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
    }

    public RestUpdateApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key/_update/{id}"), new Route(PUT, "/_security/api_key/_update/{id}"));
    }

    @Override
    public String getName() {
        return "xpack_security_update_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var apiKeyId = request.param("id");

        final var payload = PARSER.parse(request.contentParser(), null);

        final var updateApiKeyRequest = new UpdateApiKeyRequest(apiKeyId, payload.roleDescriptors, payload.metadata);

        return channel -> client.execute(UpdateApiKeyAction.INSTANCE, updateApiKeyRequest, new RestToXContentListener<>(channel));
    }

    record Payload(List<RoleDescriptor> roleDescriptors, Map<String, Object> metadata) {}
}
