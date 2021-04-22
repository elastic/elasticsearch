/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

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
                assertThat(response.getHttpCaKeystore(), is(serialized.getHttpCaKeystore()));
                assertThat(response.getTransportKeystore(), is(serialized.getTransportKeystore()));
                assertThat(response.getClusterName(), is(serialized.getClusterName()));
                assertThat(response.getNodesAddresses(), is(serialized.getNodesAddresses()));
            }
        }
    }

    @Override protected NodeEnrollmentResponse createTestInstance() {
        return new NodeEnrollmentResponse(
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLengthBetween(50, 100),
            randomAlphaOfLength(10),
            randomList(10, () -> buildNewFakeTransportAddress().toString()));
    }

    @Override protected NodeEnrollmentResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override protected boolean supportsUnknownFields() {
        return false;
    }

    private static final ParseField HTTP_CA_KEYSTORE = new ParseField("http_ca_keystore");
    private static final ParseField TRANSPORT_KEYSTORE = new ParseField("transport_keystore");
    private static final ParseField CLUSTER_NAME = new ParseField("cluster_name");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodeEnrollmentResponse, Void>
        PARSER =
        new ConstructingObjectParser<>("node_enrollment_response", true, a -> {
            final String httpCaKeystore = (String) a[0];
            final String transportKeystore = (String) a[1];
            final String clusterName = (String) a[2];
            final List<String> nodesAddresses = (List<String>) a[3];
            return new NodeEnrollmentResponse(httpCaKeystore, transportKeystore, clusterName, nodesAddresses);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_KEYSTORE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_KEYSTORE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_NAME);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), NODES_ADDRESSES);
    }
}
