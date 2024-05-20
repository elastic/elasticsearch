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
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequestTranslator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public final class RestUpdateApiKeyAction extends ApiKeyBaseRestHandler {
    private final UpdateApiKeyRequestTranslator requestTranslator;

    public RestUpdateApiKeyAction(
        final Settings settings,
        final XPackLicenseState licenseState,
        final UpdateApiKeyRequestTranslator requestTranslator
    ) {
        super(settings, licenseState);
        this.requestTranslator = requestTranslator;
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
        final UpdateApiKeyRequest updateApiKeyRequest = requestTranslator.translate(request);
        return channel -> client.execute(UpdateApiKeyAction.INSTANCE, updateApiKeyRequest, new RestToXContentListener<>(channel));
    }

}
