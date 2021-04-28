/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ClientErnollmentResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String httpCa = randomAlphaOfLength(50);
        final List<String> nodesAddresses =  randomList(10, () -> buildNewFakeTransportAddress().toString());

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject().field("http_ca", httpCa).field("nodes_addresses", nodesAddresses).endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final ClientEnrollmentResponse response = ClientEnrollmentResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getHttpCa(), equalTo(httpCa));
        assertThat(response.getNodesAddresses(), equalTo(nodesAddresses));
    }

    public void testEqualsHashCode() {
        final String httpCa = randomAlphaOfLength(50);
        final List<String> nodesAddresses =  randomList(10, () -> buildNewFakeTransportAddress().toString());
        ClientEnrollmentResponse clientEnrollmentResponse = new ClientEnrollmentResponse(httpCa, nodesAddresses);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(clientEnrollmentResponse, (original) -> {
            return new ClientEnrollmentResponse(original.getHttpCa(), original.getNodesAddresses());
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(clientEnrollmentResponse, (original) -> {
            return new ClientEnrollmentResponse(original.getHttpCa(), original.getNodesAddresses());
        }, ClientErnollmentResponseTests::mutateTestItem);
    }

    private static ClientEnrollmentResponse mutateTestItem(ClientEnrollmentResponse original) {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return new ClientEnrollmentResponse(randomAlphaOfLength(51), original.getNodesAddresses());
            case 1:
                return new ClientEnrollmentResponse(original.getHttpCa(),
                    original.getNodesAddresses().subList(0, original.getNodesAddresses().size()-1));
            default:
                return new ClientEnrollmentResponse(randomAlphaOfLength(51),
                    original.getNodesAddresses().subList(0, original.getNodesAddresses().size()-1));
        }
    }
}
