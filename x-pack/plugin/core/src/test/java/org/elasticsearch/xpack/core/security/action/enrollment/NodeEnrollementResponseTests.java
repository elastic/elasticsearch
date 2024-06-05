/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class NodeEnrollementResponseTests extends AbstractXContentTestCase<NodeEnrollmentResponse> {

    public void testSerialization() throws Exception {
        NodeEnrollmentResponse response = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeEnrollmentResponse serialized = new NodeEnrollmentResponse(in);
                assertThat(response.getHttpCaKey(), is(serialized.getHttpCaKey()));
                assertThat(response.getHttpCaCert(), is(serialized.getHttpCaCert()));
                assertThat(response.getTransportCaCert(), is(serialized.getTransportCaCert()));
                assertThat(response.getTransportKey(), is(serialized.getTransportKey()));
                assertThat(response.getTransportCert(), is(serialized.getTransportCert()));
                assertThat(response.getNodesAddresses(), is(serialized.getNodesAddresses()));
            }
        }
    }

    @Override
    protected NodeEnrollmentResponse createTestInstance() {
        return new NodeEnrollmentResponse(
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLengthBetween(50, 100),
            randomList(10, () -> buildNewFakeTransportAddress().toString())
        );
    }

    @Override
    protected NodeEnrollmentResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    private static final ParseField HTTP_CA_KEY = new ParseField("http_ca_key");
    private static final ParseField HTTP_CA_CERT = new ParseField("http_ca_cert");
    private static final ParseField TRANSPORT_CA_CERT = new ParseField("transport_ca_cert");
    private static final ParseField TRANSPORT_KEY = new ParseField("transport_key");
    private static final ParseField TRANSPORT_CERT = new ParseField("transport_cert");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodeEnrollmentResponse, Void> PARSER = new ConstructingObjectParser<>(
        "node_enrollment_response",
        true,
        a -> {
            final String httpCaKey = (String) a[0];
            final String httpCaCert = (String) a[1];
            final String transportCaCert = (String) a[2];
            final String transportKey = (String) a[3];
            final String transportCert = (String) a[4];
            final List<String> nodesAddresses = (List<String>) a[5];
            return new NodeEnrollmentResponse(httpCaKey, httpCaCert, transportCaCert, transportKey, transportCert, nodesAddresses);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_KEY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_CERT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_CA_CERT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_KEY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_CERT);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), NODES_ADDRESSES);
    }
}
