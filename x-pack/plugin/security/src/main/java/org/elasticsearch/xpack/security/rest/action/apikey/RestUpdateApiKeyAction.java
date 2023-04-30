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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

@ServerlessScope(Scope.PUBLIC)
public final class RestUpdateApiKeyAction extends ApiKeyBaseRestHandler {

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

    public RestUpdateApiKeyAction(final Settings settings, final XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_security/api_key/{ids}"));
    }

    @Override
    public String getName() {
        return "xpack_security_update_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // Note that we use `ids` here even though we only support a single id. This is because this route shares a path prefix with
        // `RestClearApiKeyCacheAction` and our current REST implementation requires that path params have the same wildcard if their paths
        // share a prefix
        final var apiKeyId = request.param("ids");
        final var payload = request.hasContent() == false ? new Payload(null, null) : PARSER.parse(request.contentParser(), null);
        return channel -> client.execute(
            UpdateApiKeyAction.INSTANCE,
            new UpdateApiKeyRequest(apiKeyId, payload.roleDescriptors, payload.metadata),
            new RestToXContentListener<>(channel)
        );
    }

    record Payload(List<RoleDescriptor> roleDescriptors, Map<String, Object> metadata) {}
}
