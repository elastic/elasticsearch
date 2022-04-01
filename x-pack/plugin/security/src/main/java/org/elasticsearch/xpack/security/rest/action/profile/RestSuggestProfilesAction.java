/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RestSuggestProfilesAction extends SecurityBaseRestHandler {

    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "suggest_profile_request_payload",
        a -> new Payload((String) a[0], (Integer) a[1])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField("name"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("size"));
    }

    public RestSuggestProfilesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/profile/_suggest"), new Route(POST, "/_security/profile/_suggest"));
    }

    @Override
    public String getName() {
        return "xpack_security_suggest_profile";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Set<String> dataKeys = Strings.tokenizeByCommaToSet(request.param("data", null));
        final Payload payload = request.hasContentOrSourceParam()
            ? PARSER.parse(request.contentOrSourceParamParser(), null)
            : new Payload(null, null);

        final SuggestProfilesRequest suggestProfilesRequest = new SuggestProfilesRequest(dataKeys, payload.name(), payload.size());
        return channel -> client.execute(SuggestProfilesAction.INSTANCE, suggestProfilesRequest, new RestToXContentListener<>(channel));
    }

    record Payload(String name, Integer size) {

        public String name() {
            return name != null ? name : "";
        }

        public Integer size() {
            return size != null ? size : 10;
        }
    }
}
