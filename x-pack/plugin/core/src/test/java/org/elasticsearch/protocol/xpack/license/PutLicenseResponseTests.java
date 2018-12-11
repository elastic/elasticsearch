/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class PutLicenseResponseTests extends
        AbstractHlrcStreamableXContentTestCase<PutLicenseResponse, org.elasticsearch.client.license.PutLicenseResponse> {

    @Override
    public org.elasticsearch.client.license.PutLicenseResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.license.PutLicenseResponse.fromXContent(parser);
    }

    @Override
    public PutLicenseResponse convertHlrcToInternal(org.elasticsearch.client.license.PutLicenseResponse instance) {
        return new PutLicenseResponse(instance.isAcknowledged(), LicensesStatus.valueOf(instance.status().name()),
            instance.acknowledgeHeader(), instance.acknowledgeMessages());
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
    protected PutLicenseResponse createTestInstance() {
        boolean acknowledged = randomBoolean();
        LicensesStatus status = randomFrom(LicensesStatus.VALID, LicensesStatus.INVALID, LicensesStatus.EXPIRED);
        String messageHeader;
        Map<String, String[]> ackMessages;
        if (randomBoolean()) {
            messageHeader = randomAlphaOfLength(10);
            ackMessages = randomAckMessages();
        } else {
            messageHeader = null;
            ackMessages = Collections.emptyMap();
        }

        return new PutLicenseResponse(acknowledged, status, messageHeader, ackMessages);
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
    protected PutLicenseResponse createBlankInstance() {
        return new PutLicenseResponse();
    }

    @Override
    protected PutLicenseResponse mutateInstance(PutLicenseResponse response) {
        @SuppressWarnings("unchecked")
        Function<PutLicenseResponse, PutLicenseResponse> mutator = randomFrom(
            r -> new PutLicenseResponse(
                r.isAcknowledged() == false,
                r.status(),
                r.acknowledgeHeader(),
                r.acknowledgeMessages()),
            r -> new PutLicenseResponse(
                r.isAcknowledged(),
                mutateStatus(r.status()),
                r.acknowledgeHeader(),
                r.acknowledgeMessages()),
            r -> {
                if (r.acknowledgeMessages().isEmpty()) {
                    return new PutLicenseResponse(
                        r.isAcknowledged(),
                        r.status(),
                        randomAlphaOfLength(10),
                        randomAckMessages()
                    );
                } else {
                    return new PutLicenseResponse(r.isAcknowledged(), r.status());
                }
            }

        );
        return mutator.apply(response);
    }

    private LicensesStatus mutateStatus(LicensesStatus status) {
        return randomValueOtherThan(status, () -> randomFrom(LicensesStatus.values()));
    }
}
