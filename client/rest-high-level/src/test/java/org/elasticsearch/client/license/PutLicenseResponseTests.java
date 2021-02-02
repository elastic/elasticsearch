/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.license;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PutLicenseResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.protocol.xpack.license.PutLicenseResponse, PutLicenseResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.license.PutLicenseResponse createServerTestInstance(XContentType xContentType) {
        boolean acknowledged = randomBoolean();
        org.elasticsearch.protocol.xpack.license.LicensesStatus status =
            randomFrom(org.elasticsearch.protocol.xpack.license.LicensesStatus.VALID,
                org.elasticsearch.protocol.xpack.license.LicensesStatus.INVALID,
                org.elasticsearch.protocol.xpack.license.LicensesStatus.EXPIRED);
        String messageHeader;
        Map<String, String[]> ackMessages;
        if (randomBoolean()) {
            messageHeader = randomAlphaOfLength(10);
            ackMessages = randomAckMessages();
        } else {
            messageHeader = null;
            ackMessages = Collections.emptyMap();
        }

        return new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(acknowledged, status, messageHeader, ackMessages);
    }

    private static Map<String, String[]> randomAckMessages() {
        int nFeatures = randomIntBetween(1, 5);

        Map<String, String[]> ackMessages = new HashMap<>();

        for (int i = 0; i < nFeatures; i++) {
            String feature = randomAlphaOfLengthBetween(9, 15);
            int nMessages = randomIntBetween(1, 5);
            String[] messages = new String[nMessages];
            for (int j = 0; j < nMessages; j++) {
                messages[j] = randomAlphaOfLengthBetween(10, 30);
            }
            ackMessages.put(feature, messages);
        }

        return ackMessages;
    }

    @Override
    protected PutLicenseResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return PutLicenseResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.protocol.xpack.license.PutLicenseResponse serverTestInstance,
                                   PutLicenseResponse clientInstance) {
        assertThat(serverTestInstance.status().name(), equalTo(clientInstance.status().name()));
        assertThat(serverTestInstance.acknowledgeHeader(), equalTo(clientInstance.acknowledgeHeader()));
        assertThat(serverTestInstance.acknowledgeMessages().keySet(), equalTo(clientInstance.acknowledgeMessages().keySet()));
        for(Map.Entry<String, String[]> entry: serverTestInstance.acknowledgeMessages().entrySet()) {
            assertTrue(Arrays.equals(entry.getValue(), clientInstance.acknowledgeMessages().get(entry.getKey())));
        }
    }
}
