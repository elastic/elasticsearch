/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileUserAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileUserRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RestUpdateProfileUserAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "update_profile_user_request_payload",
        a -> new Payload((String) a[0], (String) a[1], (String) a[2], (Boolean) a[3])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField("email"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("full_name"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("display_name"));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("active"));
    }

    public RestUpdateProfileUserAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/profile/_user/{uid}"));
    }

    @Override
    public String getName() {
        return "xpack_security_update_profile_user";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String uid = request.param("uid");
        final long ifPrimaryTerm = request.paramAsLong("if_primary_term", -1);
        final long ifSeqNo = request.paramAsLong("if_seq_no", -1);
        final RefreshPolicy refreshPolicy = RefreshPolicy.parse(request.param("refresh", "wait_for"));
        final Payload payload = PARSER.parse(request.contentParser(), null);

        final UpdateProfileUserRequest updateProfileUserRequest = new UpdateProfileUserRequest(
            uid,
            payload.email,
            payload.fullName,
            payload.displayName,
            payload.active,
            ifPrimaryTerm,
            ifSeqNo,
            refreshPolicy
        );

        return channel -> client.execute(UpdateProfileUserAction.INSTANCE, updateProfileUserRequest, new RestToXContentListener<>(channel));
    }

    record Payload(@Nullable String email, @Nullable String fullName, @Nullable String displayName, @Nullable Boolean active) {}

}
