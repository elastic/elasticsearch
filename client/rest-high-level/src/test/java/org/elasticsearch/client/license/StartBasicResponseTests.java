/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.license;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.common.ProtocolUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class StartBasicResponseTests extends ESTestCase {

    @Test
    public void testFromXContent() throws Exception {
        StartBasicResponse.Status status = randomFrom(StartBasicResponse.Status.values());

        boolean acknowledged = status != StartBasicResponse.Status.NEED_ACKNOWLEDGEMENT;
        String acknowledgeMessage = null;
        Map<String, String[]> ackMessages = Collections.emptyMap();
        if (status != StartBasicResponse.Status.GENERATED_BASIC) {
            acknowledgeMessage = randomAlphaOfLength(10);
            ackMessages = randomAckMessages();
        }

        final StartBasicResponse startBasicResponse = new StartBasicResponse(status, ackMessages, acknowledgeMessage);

        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xContentType);

        toXContent(startBasicResponse, builder);

        final StartBasicResponse response = StartBasicResponse.fromXContent(createParser(builder));
        assertThat(response.isAcknowledged(), equalTo(acknowledged));
        assertThat(response.isBasicStarted(), equalTo(status.isBasicStarted()));
        assertThat(response.getAcknowledgeMessage(), equalTo(acknowledgeMessage));
        assertThat(ProtocolUtils.equals(response.getAcknowledgeMessages(), ackMessages), equalTo(true));
    }

    private static void toXContent(StartBasicResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("acknowledged", response.isAcknowledged());
        if (response.isBasicStarted()) {
            builder.field("basic_was_started", true);
        } else {
            builder.field("basic_was_started", false);
            builder.field("error_message", response.getErrorMessage());
        }
        if (response.getAcknowledgeMessages().isEmpty() == false) {
            builder.startObject("acknowledge");
            builder.field("message", response.getAcknowledgeMessage());
            for (Map.Entry<String, String[]> entry : response.getAcknowledgeMessages().entrySet()) {
                builder.startArray(entry.getKey());
                for (String message : entry.getValue()) {
                    builder.value(message);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
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

}
