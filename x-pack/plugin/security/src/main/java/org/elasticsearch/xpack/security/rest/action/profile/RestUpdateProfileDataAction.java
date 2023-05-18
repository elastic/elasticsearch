/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

@ServerlessScope(Scope.INTERNAL)
public class RestUpdateProfileDataAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "update_profile_data_request_payload",
        a -> new Payload((Map<String, Object>) a[0], (Map<String, Object>) a[1])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("labels"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("data"));
    }

    public RestUpdateProfileDataAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_security/profile/{uid}/_data"), new Route(POST, "/_security/profile/{uid}/_data"));
    }

    @Override
    public String getName() {
        return "xpack_security_update_profile_data";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String uid = request.param("uid");
        final long ifPrimaryTerm = request.paramAsLong("if_primary_term", -1);
        final long ifSeqNo = request.paramAsLong("if_seq_no", -1);
        final RefreshPolicy refreshPolicy = RefreshPolicy.parse(request.param("refresh", "wait_for"));
        final Payload payload = PARSER.parse(request.contentParser(), null);

        final UpdateProfileDataRequest updateProfileDataRequest = new UpdateProfileDataRequest(
            uid,
            payload.labels,
            payload.data,
            ifPrimaryTerm,
            ifSeqNo,
            refreshPolicy
        );

        return channel -> client.execute(UpdateProfileDataAction.INSTANCE, updateProfileDataRequest, new RestToXContentListener<>(channel));
    }

    record Payload(Map<String, Object> labels, Map<String, Object> data) {}
}
