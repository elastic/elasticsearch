/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestActivateProfileAction extends SecurityBaseRestHandler implements RestRequestFilter {

    static final ObjectParser<ActivateProfileRequest, Void> PARSER = new ObjectParser<>(
        "activate_profile_request",
        ActivateProfileRequest::new
    );
    static {
        PARSER.declareString((req, str) -> req.getGrant().setType(str), new ParseField("grant_type"));
        PARSER.declareString((req, str) -> req.getGrant().setUsername(str), new ParseField("username"));
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setPassword(secStr),
            RestActivateProfileAction::getSecureString,
            new ParseField("password"),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setAccessToken(secStr),
            RestActivateProfileAction::getSecureString,
            new ParseField("access_token"),
            ObjectParser.ValueType.STRING
        );
    }

    public RestActivateProfileAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/profile/_activate"));
    }

    @Override
    public String getName() {
        return "xpack_security_activate_profile";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final ActivateProfileRequest activateProfileRequest = PARSER.parse(parser, null);
            return channel -> client.execute(ActivateProfileAction.INSTANCE, activateProfileRequest, new RestToXContentListener<>(channel));
        }
    }

    // TODO: extract to standalone helper method
    private static SecureString getSecureString(XContentParser parser) throws IOException {
        return new SecureString(
            Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())
        );
    }

    @Override
    public Set<String> getFilteredFields() {
        return Set.of("password", "access_token");
    }
}
