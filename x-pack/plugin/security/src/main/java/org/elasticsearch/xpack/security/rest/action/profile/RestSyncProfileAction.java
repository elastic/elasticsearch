/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.profile.SyncProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.SyncProfileRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public final class RestSyncProfileAction extends SecurityBaseRestHandler implements RestRequestFilter {

    static final ObjectParser<SyncProfileRequest, Void> PARSER = new ObjectParser<>("sync_profile_request", SyncProfileRequest::new);
    static {
        PARSER.declareString((req, str) -> req.getGrant().setType(str), new ParseField("grant_type"));
        PARSER.declareString((req, str) -> req.getGrant().setUsername(str), new ParseField("username"));
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setPassword(secStr),
            RestSyncProfileAction::getSecureString,
            new ParseField("password"),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setAccessToken(secStr),
            RestSyncProfileAction::getSecureString,
            new ParseField("access_token"),
            ObjectParser.ValueType.STRING
        );
    }

    private static SecureString getSecureString(XContentParser parser) throws IOException {
        return new SecureString(
            Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())
        );
    }

    public RestSyncProfileAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/profile/_sync"), new Route(PUT, "/_security/profile/_sync"));
    }

    @Override
    public String getName() {
        return "xpack_security_sync_profile";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SyncProfileRequest syncProfileRequest = PARSER.parse(parser, null);
            return channel -> client.execute(
                SyncProfileAction.INSTANCE,
                syncProfileRequest,
                new RestToXContentListener<AcknowledgedResponse>(channel).delegateResponse((listener, ex) -> {
                    RestStatus status = ExceptionsHelper.status(ex);
                    if (status == RestStatus.UNAUTHORIZED) {
                        listener.onFailure(new ElasticsearchSecurityException("Failed to sync profile", RestStatus.FORBIDDEN, ex));
                    } else {
                        listener.onFailure(ex);
                    }
                })
            );
        }
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("password", "access_token");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
