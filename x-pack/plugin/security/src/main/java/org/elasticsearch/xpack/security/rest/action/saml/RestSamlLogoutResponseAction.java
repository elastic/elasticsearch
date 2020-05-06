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
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutResponseRequestBuilder;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutResponseResponse;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSamlLogoutResponseAction extends SamlBaseRestHandler{

    private static final Logger logger = LogManager.getLogger(RestSamlLogoutResponseAction.class);

    static class Input {
        String content;
        List<String> ids;
        String realm;
        String assertionConsumerServiceURL;

        void setContent(String content) {
            this.content = content;
        }

        void setIds(List<String> ids) {
            this.ids = ids;
        }

        void setRealm(String realm) { this.realm = realm;}

        void setAssertionConsumerServiceURL(String assertionConsumerServiceURL) {
            this.assertionConsumerServiceURL = assertionConsumerServiceURL;
        }
    }

    static final ObjectParser<RestSamlLogoutResponseAction.Input, Void>
        PARSER = new ObjectParser<>("saml_logout_response", RestSamlLogoutResponseAction.Input::new);

    static {
        PARSER.declareString(RestSamlLogoutResponseAction.Input::setContent, new ParseField("content"));
        PARSER.declareStringArray(RestSamlLogoutResponseAction.Input::setIds, new ParseField("ids"));
        PARSER.declareStringOrNull(RestSamlLogoutResponseAction.Input::setRealm, new ParseField("realm"));
        PARSER.declareString(RestSamlLogoutResponseAction.Input::setAssertionConsumerServiceURL, new ParseField("acs"));
    }

    public RestSamlLogoutResponseAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_saml_logout_response_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/saml/logout_response"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final RestSamlLogoutResponseAction.Input input = PARSER.parse(parser, null);
            logger.trace("SAML LogoutResponse: [{}...] [{}]", Strings.cleanTruncate(input.content, 128), input.ids);
            return channel -> {
                final byte[] bytes = decodeBase64(input.content);
                final SamlLogoutResponseRequestBuilder requestBuilder =
                    new SamlLogoutResponseRequestBuilder(client).saml(bytes).validRequestIds(input.ids).authenticatingRealm(input.realm);
                requestBuilder.execute(new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(SamlLogoutResponseResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject()
                            .endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
            };
        }
    }

    private byte[] decodeBase64(String content) {
        content = content.replaceAll("\\s+", "");
        try {
            return Base64.getDecoder().decode(content);
        } catch (IllegalArgumentException e) {
            logger.info("Failed to decode base64 string [{}] - {}", content, e.toString());
            throw e;
        }
    }
}
