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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class RestBulkUpdateApiKeyAction extends ApiKeyBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<BulkUpdateApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>(
        "bulk_update_api_key_request",
        a -> new BulkUpdateApiKeyRequest((List<String>) a[0], (List<RoleDescriptor>) a[1], (Map<String, Object>) a[2])
    );

    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("ids"));
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return RoleDescriptor.parse(n, p, false);
        }, new ParseField("role_descriptors"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
    }

    public RestBulkUpdateApiKeyAction(final Settings settings, final XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key/_bulk_update"));
    }

    @Override
    public String getName() {
        return "xpack_security_bulk_update_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final BulkUpdateApiKeyRequest parsed = PARSER.parse(parser, null);
            return channel -> client.execute(BulkUpdateApiKeyAction.INSTANCE, parsed, new RestToXContentListener<>(channel));
        }
    }
}
