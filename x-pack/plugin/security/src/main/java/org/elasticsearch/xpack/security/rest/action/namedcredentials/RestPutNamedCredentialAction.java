/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.namedcredentials;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.namedcredentials.PutNamedCredentialAction;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for {@code PUT /_security/named_credentials/{name}}.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestPutNamedCredentialAction extends NamedCredentialsBaseRestHandler implements RestRequestFilter {

    public RestPutNamedCredentialAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_put_named_credential";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_security/named_credentials/{name}"));
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("auth");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final PutNamedCredentialAction.Request req;
        try (var parser = request.contentParser()) {
            req = PutNamedCredentialAction.Request.fromXContent(name, parser);
        }
        return channel -> client.execute(
            PutNamedCredentialAction.INSTANCE,
            req,
            new RestToXContentListener<>(channel, PutNamedCredentialAction.Response::status, r -> null)
        );
    }
}
