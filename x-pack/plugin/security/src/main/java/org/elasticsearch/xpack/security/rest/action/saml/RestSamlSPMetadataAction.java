/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
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
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlSPMetadataResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSamlSPMetadataAction extends SamlBaseRestHandler {

    static class Input {
        String realm;
        void setRealm(String realm) {
            this.realm = realm;
        }
    }

    static final ObjectParser<SamlSPMetadataRequest, Void> PARSER = new ObjectParser<>("security_saml_metadata",
        SamlSPMetadataRequest::new);

    static {
        PARSER.declareStringOrNull(SamlSPMetadataRequest::setRealmName, new ParseField("realm"));
    }

    public RestSamlSPMetadataAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(GET, "/_security/saml/metadata"));
    }

    @Override
    public String getName() {
        return "security_saml_metadata_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final SamlSPMetadataRequest SamlSPRequest = PARSER.parse(parser, null);
            return channel -> client.execute(SamlSPMetadataAction.INSTANCE, SamlSPRequest,
                new RestBuilderListener<SamlSPMetadataResponse>(channel) {
                @Override
                public RestResponse buildResponse(SamlSPMetadataResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("xml_metadata", response.getXMLString());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }
    }
}
