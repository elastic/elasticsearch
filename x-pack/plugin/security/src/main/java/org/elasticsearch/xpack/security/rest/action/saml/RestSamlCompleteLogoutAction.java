/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.saml.SamlCompleteLogoutRequestBuilder;
import org.elasticsearch.xpack.core.security.action.saml.SamlCompleteLogoutResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * This Rest endpoint handles SAML LogoutResponse sent from idP with either HTTP-Redirect or HTTP-Post binding.
 * For HTTP-Redirect binding, it expects {@link Input#queryString} be set to the query string of the redirect URI.
 * For HTTP-Post binding, it expects {@link Input#content} be set to the value of SAMLResponse form parameter, i.e.
 * caller of this API must do the work to extract the SAMLResponse value from body of the HTTP-Post request. The
 * value must also be URL decoded if necessary.
 */
public class RestSamlCompleteLogoutAction extends SamlBaseRestHandler{

    private static final Logger logger = LogManager.getLogger(RestSamlCompleteLogoutAction.class);

    static class Input {
        String queryString;
        String content;
        List<String> ids;
        String realm;

        void setQueryString(String queryString) {
            this.queryString = queryString;
        }

        void setContent(String content) {
            this.content = content;
        }

        void setIds(List<String> ids) {
            this.ids = ids;
        }

        void setRealm(String realm) { this.realm = realm;}
    }

    static final ObjectParser<RestSamlCompleteLogoutAction.Input, Void>
        PARSER = new ObjectParser<>("saml_complete_logout", RestSamlCompleteLogoutAction.Input::new);

    static {
        PARSER.declareStringOrNull(RestSamlCompleteLogoutAction.Input::setQueryString, new ParseField("queryString"));
        PARSER.declareStringOrNull(RestSamlCompleteLogoutAction.Input::setContent, new ParseField("content"));
        PARSER.declareStringArray(RestSamlCompleteLogoutAction.Input::setIds, new ParseField("ids"));
        PARSER.declareString(RestSamlCompleteLogoutAction.Input::setRealm, new ParseField("realm"));
    }

    public RestSamlCompleteLogoutAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_saml_complete_logout_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/saml/complete_logout"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final RestSamlCompleteLogoutAction.Input input = PARSER.parse(parser, null);
            logger.trace("SAML LogoutResponse: [{}...] [{}...] [{}]",
                Strings.cleanTruncate(input.queryString, 128), Strings.cleanTruncate(input.content, 128), input.ids);
            return channel -> {
                final SamlCompleteLogoutRequestBuilder requestBuilder =
                    new SamlCompleteLogoutRequestBuilder(client)
                        .queryString(input.queryString).content(input.content)
                        .validRequestIds(input.ids).authenticatingRealm(input.realm);
                requestBuilder.execute(new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlCompleteLogoutResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject()
                            .endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
            };
        }
    }
}
