/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Rest action to create an API key specific to cross cluster access via the dedicate remote cluster server port
 */
public final class RestCreateCrossClusterApiKeyAction extends ApiKeyBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<CreateCrossClusterApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>(
        "cross_cluster_api_key_request",
        false,
        (args, v) -> new CreateCrossClusterApiKeyRequest(
            (String) args[0],
            (CrossClusterApiKeyRoleDescriptorBuilder) args[1],
            TimeValue.parseTimeValue((String) args[2], null, "expiration"),
            (Map<String, Object>) args[3]
        )
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareObject(constructorArg(), CrossClusterApiKeyRoleDescriptorBuilder.PARSER, new ParseField("access"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("metadata"));
    }

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestCreateCrossClusterApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/cross_cluster/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_create_cross_cluster_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final CreateCrossClusterApiKeyRequest createCrossClusterApiKeyRequest = PARSER.parse(request.contentParser(), null);
        return channel -> client.execute(
            CreateCrossClusterApiKeyAction.INSTANCE,
            createCrossClusterApiKeyRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
