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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public final class RestUpdateApiKeyAction extends ApiKeyBaseRestHandler {
    private final UpdateApiKeyRequest.RequestTranslator requestTranslator;

    public RestUpdateApiKeyAction(
        final Settings settings,
        final XPackLicenseState licenseState,
        final UpdateApiKeyRequest.RequestTranslator requestTranslator
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
        // TODO this needs to be a supplier since the setting is dynamic...
        final boolean strictRequestValidation = settings.getAsBoolean(
            "xpack.security.authc.api_key.strict_request_validation.enabled",
            false
        );
        final UpdateApiKeyRequest updateApiKeyRequest = requestTranslator.translate(request, strictRequestValidation);
        return channel -> client.execute(UpdateApiKeyAction.INSTANCE, updateApiKeyRequest, new RestToXContentListener<>(channel));
    }

}
