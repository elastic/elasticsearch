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
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RestSuggestProfilesAction extends SecurityBaseRestHandler {

    static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "suggest_profile_request_payload",
        a -> new Payload((String) a[0], (Integer) a[1], (PayloadHint) a[2], (String) a[3])
    );

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<PayloadHint, Void> HINT_PARSER = new ConstructingObjectParser<>(
        "suggest_profile_request_payload_hint",
        a -> new PayloadHint((List<String>) a[0], (Map<String, Object>) a[1])
    );

    static {
        HINT_PARSER.declareStringArray(optionalConstructorArg(), new ParseField("uids"));
        HINT_PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField("labels"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("name"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("size"));
        PARSER.declareObject(optionalConstructorArg(), HINT_PARSER, new ParseField("hint"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("data"));
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
        final Payload payload = request.hasContentOrSourceParam()
            ? PARSER.parse(request.contentOrSourceParamParser(), null)
            : new Payload(null, null, null, null);

        final String data = request.param("data", null);
        if (data != null && payload.data() != null) {
            throw new IllegalArgumentException(
                "The [data] parameter must be specified in either request body or as query parameter, but not both"
            );
        }
        final Set<String> dataKeys = Strings.tokenizeByCommaToSet(data != null ? data : payload.data());

        final SuggestProfilesRequest suggestProfilesRequest = new SuggestProfilesRequest(
            dataKeys,
            payload.name(),
            payload.size(),
            payload.hint() == null ? null : new SuggestProfilesRequest.Hint(payload.hint().uids(), payload.hint().labels())
        );
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).execute(
            SuggestProfilesAction.INSTANCE,
            suggestProfilesRequest,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        final Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        }
        if (Security.USER_PROFILE_COLLABORATION_FEATURE.check(licenseState)) {
            return null;
        } else {
            return LicenseUtils.newComplianceException(Security.USER_PROFILE_COLLABORATION_FEATURE.getName());
        }
    }

    record Payload(String name, Integer size, PayloadHint hint, String data) {

        public String name() {
            return name != null ? name : "";
        }

        public Integer size() {
            return size != null ? size : 10;
        }
    }

    record PayloadHint(List<String> uids, Map<String, Object> labels) {}

}
