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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.namedcredentials.DecryptNamedCredentialAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for {@code GET /_security/named_credentials/{name}/_decrypt}.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDecryptNamedCredentialAction extends NamedCredentialsBaseRestHandler {

    public RestDecryptNamedCredentialAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_decrypt_named_credential";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/named_credentials/{name}/_decrypt"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final DecryptNamedCredentialAction.Request req = new DecryptNamedCredentialAction.Request(request.param("name"));
        return channel -> client.execute(DecryptNamedCredentialAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
