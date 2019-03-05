/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.PostStartBasicResponse;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class StartBasicResponseTests extends
        AbstractHlrcStreamableXContentTestCase<PostStartBasicResponse, org.elasticsearch.client.license.StartBasicResponse> {

    @Override
    public org.elasticsearch.client.license.StartBasicResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.license.StartBasicResponse.fromXContent(parser);
    }

    @Override
    public PostStartBasicResponse convertHlrcToInternal(org.elasticsearch.client.license.StartBasicResponse instance) {
        return new PostStartBasicResponse(PostStartBasicResponse.Status.valueOf(instance.getStatus().name()),
            instance.getAcknowledgeMessages(), instance.getAcknowledgeMessage());
    }

    @Override
    protected PostStartBasicResponse createBlankInstance() {
        return new PostStartBasicResponse();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // The structure of the response is such that unknown fields inside acknowledge cannot be supported since they
        // are treated as messages from new services
        return p -> p.startsWith("acknowledge");
    }

    @Override
    protected PostStartBasicResponse createTestInstance() {
        PostStartBasicResponse.Status status = randomFrom(PostStartBasicResponse.Status.values());
        String acknowledgeMessage = null;
        Map<String, String[]> ackMessages = Collections.emptyMap();
        if (status != PostStartBasicResponse.Status.GENERATED_BASIC) {
            acknowledgeMessage = randomAlphaOfLength(10);
            ackMessages = randomAckMessages();
        }
        final PostStartBasicResponse postStartBasicResponse = new PostStartBasicResponse(status, ackMessages, acknowledgeMessage);
        logger.info("{}", Strings.toString(postStartBasicResponse));
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
}
