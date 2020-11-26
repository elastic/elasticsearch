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

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.PostStartBasicResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StartBasicResponseTests extends AbstractResponseTestCase<
    PostStartBasicResponse, StartBasicResponse> {

    @Override
    protected PostStartBasicResponse createServerTestInstance(XContentType xContentType) {
        PostStartBasicResponse.Status status = randomFrom(PostStartBasicResponse.Status.values());
        String acknowledgeMessage = null;
        Map<String, String[]> ackMessages = Collections.emptyMap();
        if (status != PostStartBasicResponse.Status.GENERATED_BASIC) {
            acknowledgeMessage = randomAlphaOfLength(10);
            ackMessages = randomAckMessages();
        }
        final PostStartBasicResponse postStartBasicResponse = new PostStartBasicResponse(status, ackMessages, acknowledgeMessage);
        return postStartBasicResponse;
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
    protected StartBasicResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return StartBasicResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(PostStartBasicResponse serverTestInstance, StartBasicResponse clientInstance) {
        assertThat(serverTestInstance.getStatus().name(), equalTo(clientInstance.getStatus().name()));
        assertThat(serverTestInstance.getStatus().isBasicStarted(), equalTo(clientInstance.isBasicStarted()));
        assertThat(serverTestInstance.isAcknowledged(), equalTo(clientInstance.isAcknowledged()));
        assertThat(serverTestInstance.getStatus().getErrorMessage(), equalTo(clientInstance.getErrorMessage()));
        assertThat(serverTestInstance.getAcknowledgeMessage(), equalTo(clientInstance.getAcknowledgeMessage()));
        assertThat(serverTestInstance.getAcknowledgeMessages().keySet(), equalTo(clientInstance.getAcknowledgeMessages().keySet()));
        for(Map.Entry<String, String[]> entry: serverTestInstance.getAcknowledgeMessages().entrySet()) {
            assertTrue(Arrays.equals(entry.getValue(), clientInstance.getAcknowledgeMessages().get(entry.getKey())));
        }
    }
}
