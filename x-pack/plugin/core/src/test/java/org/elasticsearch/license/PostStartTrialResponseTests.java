/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PostStartTrialResponseTests extends AbstractStreamableXContentTestCase<PostStartTrialResponse> {

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
    protected PostStartTrialResponse doParseInstance(XContentParser parser) throws IOException {
        return PostStartTrialResponse.fromXContent(parser);
    }

    @Override
    protected PostStartTrialResponse createBlankInstance() {
        return new PostStartTrialResponse();
    }

    @Override
    protected PostStartTrialResponse createTestInstance() {
        final PostStartTrialResponse.Status status = randomFrom(PostStartTrialResponse.Status.values());
        final String licenseType = randomAlphaOfLength(10);

        final String acknowledgeHeader;
        final Map<String, String[]> acknowledgeMessages;
        if (randomBoolean()) {
            acknowledgeHeader = null;
            acknowledgeMessages = Collections.emptyMap();
        } else {
            acknowledgeHeader = randomAlphaOfLength(25);
            acknowledgeMessages = randomAckMessages();
        }

        return new PostStartTrialResponse(status, licenseType, acknowledgeMessages, acknowledgeHeader);
    }

    @Override
    protected PostStartTrialResponse mutateInstance(PostStartTrialResponse instance) throws IOException {
        Supplier<PostStartTrialResponse> mutator = randomFrom(

            () -> new PostStartTrialResponse(instance.getStatus(), instance.getLicenseType() + randomAlphaOfLength(4)),

            () -> new PostStartTrialResponse(
                    randomValueOtherThan(instance.getStatus(), () -> randomFrom(PostStartTrialResponse.Status.values())),
                    instance.getLicenseType()),

            () -> new PostStartTrialResponse(
                instance.getStatus(),
                instance.getLicenseType(),
                randomValueOtherThan(instance.getAcknowledgementMessages(), PostStartTrialResponseTests::randomAckMessages),
                null),

            () -> {
                if (instance.getAcknowledgementMessages().isEmpty()) {
                    return new PostStartTrialResponse(
                        instance.getStatus(),
                        instance.getLicenseType(),
                        randomAckMessages(),
                        randomAlphaOfLength(10));
                } else {
                    return new PostStartTrialResponse(instance.getStatus(), instance.getLicenseType());
                }
            }
        );

        return mutator.get();
    }

    // todo when PutLicenseResponseTests return to this project, consolidate this method with its version
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
