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
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequestTranslator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public final class RestBulkUpdateApiKeyAction extends ApiKeyBaseRestHandler {

    private final BulkUpdateApiKeyRequestTranslator requestTranslator;

    public RestBulkUpdateApiKeyAction(
        final Settings settings,
        final XPackLicenseState licenseState,
        final BulkUpdateApiKeyRequestTranslator requestTranslator
    ) {
        super(settings, licenseState);
        this.requestTranslator = requestTranslator;
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
        final BulkUpdateApiKeyRequest bulkUpdateApiKeyRequest = requestTranslator.translate(request);
        return channel -> client.execute(BulkUpdateApiKeyAction.INSTANCE, bulkUpdateApiKeyRequest, new RestToXContentListener<>(channel));
    }
}
