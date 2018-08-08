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

package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class PostStartTrialResponseTests extends AbstractXContentTestCase<PostStartTrialResponse> {

    @Override
    protected PostStartTrialResponse createTestInstance() {
        final boolean acknowledged = randomBoolean();

        final boolean successfulStartedTrial = randomBoolean();

        if (successfulStartedTrial) {
            final String licenseType = randomAlphaOfLengthBetween(3, 20);
            return new PostStartTrialResponse(acknowledged, licenseType);
        } else {
            final String errorMessage = randomAlphaOfLengthBetween(3, 20);
            final String acknowledgeHeader = randomAlphaOfLengthBetween(3, 20);
            final Map<String, String[]> acknowledgeMessages = randomAckMessages();
            return new PostStartTrialResponse(acknowledged, errorMessage, acknowledgeHeader, acknowledgeMessages);
        }
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
    protected PostStartTrialResponse doParseInstance(XContentParser parser) throws IOException {
        return PostStartTrialResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("acknowledge");
    }
}
