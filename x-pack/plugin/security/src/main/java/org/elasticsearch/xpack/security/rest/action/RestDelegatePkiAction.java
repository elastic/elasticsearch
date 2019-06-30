/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.DelegatePkiRequest;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.TokenBaseRestHandler;
import org.elasticsearch.xpack.core.security.action.DelegatePkiResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestDelegatePkiAction extends TokenBaseRestHandler {

    private static final ParseField X5C_FIELD = new ParseField("x5c");

    static final ConstructingObjectParser<DelegatePkiRequest, Void> PARSER = new ConstructingObjectParser<>("delegate_pki", true, a -> {
        final List<String> encodedCertificatesList = (List<String>) a[0];
        final X509Certificate[] certificates = new X509Certificate[encodedCertificatesList.size()];
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            for (int i = 0; i < encodedCertificatesList.size(); i++) {
                try (ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(encodedCertificatesList.get(i)))) {
                    certificates[i] = (X509Certificate) certificateFactory.generateCertificate(bis);
                } catch (CertificateException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
        return new DelegatePkiRequest(certificates);
    });

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), X5C_FIELD);
    }

    public RestDelegatePkiAction(Settings settings, RestController controller, XPackLicenseState xPackLicenseState) {
        super(settings, xPackLicenseState);
        controller.registerHandler(POST, "/_security/delegate_pki", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final DelegatePkiRequest delegatePkiRequest = PARSER.parse(parser, null);
            return channel -> client.execute(TransportDelegatePkiAction.TYPE, delegatePkiRequest,
                    new RestBuilderListener<DelegatePkiResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(DelegatePkiResponse delegatePkiResponse, XContentBuilder builder)
                                throws Exception {
                            delegatePkiResponse.toXContent(builder, channel.request());
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    });
        }
    }

    @Override
    public String getName() {
        return "delegate_pki_action";
    }
}
