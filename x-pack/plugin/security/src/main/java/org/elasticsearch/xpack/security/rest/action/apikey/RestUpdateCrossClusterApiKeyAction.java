/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.security.Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE;

public final class RestUpdateCrossClusterApiKeyAction extends ApiKeyBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "update_cross_cluster_api_key_request_payload",
        a -> new Payload((CrossClusterApiKeyRoleDescriptorBuilder) a[0], (Map<String, Object>) a[1])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), CrossClusterApiKeyRoleDescriptorBuilder.PARSER, new ParseField("access"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
    }

    public RestUpdateCrossClusterApiKeyAction(final Settings settings, final XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_security/cross_cluster/api_key/{id}"));
    }

    @Override
    public String getName() {
        return "xpack_security_update_cross_cluster_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var apiKeyId = request.param("id");
        final Payload payload = PARSER.parse(request.contentParser(), null);

        return channel -> client.execute(
            UpdateCrossClusterApiKeyAction.INSTANCE,
            new UpdateCrossClusterApiKeyRequest(apiKeyId, payload.builder, payload.metadata),
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        final Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.checkWithoutTracking(licenseState)) {
            return null;
        } else {
            return LicenseUtils.newComplianceException(ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.getName());
        }
    }

    record Payload(CrossClusterApiKeyRoleDescriptorBuilder builder, Map<String, Object> metadata) {}
}
