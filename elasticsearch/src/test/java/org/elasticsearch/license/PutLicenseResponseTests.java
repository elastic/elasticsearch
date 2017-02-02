/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PutLicenseResponseTests extends ESTestCase {
    public void testSerialization() throws Exception {
        boolean acknowledged = randomBoolean();
        LicensesStatus status = randomFrom(LicensesStatus.VALID, LicensesStatus.INVALID, LicensesStatus.EXPIRED);
        Map<String, String[]> ackMessages = randomAckMessages();

        PutLicenseResponse response = new PutLicenseResponse(acknowledged, status, "", ackMessages);

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();

        Map<String, Object> map = XContentHelper.convertToMap(contentBuilder.bytes(), false, contentBuilder.contentType()).v2();
        assertThat(map.containsKey("acknowledged"), equalTo(true));
        boolean actualAcknowledged = (boolean) map.get("acknowledged");
        assertThat(actualAcknowledged, equalTo(acknowledged));

        assertThat(map.containsKey("license_status"), equalTo(true));
        String actualStatus = (String) map.get("license_status");
        assertThat(actualStatus, equalTo(status.name().toLowerCase(Locale.ROOT)));

        assertThat(map.containsKey("acknowledge"), equalTo(true));
        Map<String, List<String>> actualAckMessages = (HashMap) map.get("acknowledge");
        assertTrue(actualAckMessages.containsKey("message"));
        actualAckMessages.remove("message");
        assertThat(actualAckMessages.keySet(), equalTo(ackMessages.keySet()));
        for (Map.Entry<String, List<String>> entry : actualAckMessages.entrySet()) {
            assertArrayEquals(entry.getValue().toArray(), ackMessages.get(entry.getKey()));
        }
    }

    public void testStreamSerialization() throws IOException {
        boolean acknowledged = randomBoolean();
        LicensesStatus status = randomFrom(LicensesStatus.VALID, LicensesStatus.INVALID, LicensesStatus.EXPIRED);
        Map<String, String[]> ackMessages = randomAckMessages();

        // write the steam so that we can attempt to read it back
        BytesStreamOutput output = new BytesStreamOutput();

        PutLicenseResponse response = new PutLicenseResponse(acknowledged, status, "", ackMessages);
        // write it out
        response.writeTo(output);

        StreamInput input = output.bytes().streamInput();

        // read it back in
        response.readFrom(input);

        assertThat(response.isAcknowledged(), equalTo(acknowledged));
        assertThat(response.status(), equalTo(status));
        assertThat(response.acknowledgeMessages(), not(sameInstance(ackMessages)));
        assertThat(response.acknowledgeMessages().size(), equalTo(ackMessages.size()));

        for (String key : ackMessages.keySet()) {
            assertArrayEquals(ackMessages.get(key), response.acknowledgeMessages().get(key));
        }
    }

    private Map<String, String[]> randomAckMessages() {
        int nFeatures = randomIntBetween(1, 5);

        Map<String, String[]> ackMessages = new HashMap<>();

        for (int i = 0; i < nFeatures; i++) {
            String feature = randomAsciiOfLengthBetween(9, 15);
            int nMessages = randomIntBetween(1, 5);
            String[] messages = new String[nMessages];
            for (int j = 0; j < nMessages; j++) {
                messages[j] = randomAsciiOfLengthBetween(10, 30);
            }
            ackMessages.put(feature, messages);
        }

        return ackMessages;
    }
}
