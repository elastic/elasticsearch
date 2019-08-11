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

import org.elasticsearch.client.AbstractHlrcWriteableXContentTestCase;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class PutLicenseResponseTests extends AbstractHlrcWriteableXContentTestCase<
    org.elasticsearch.protocol.xpack.license.PutLicenseResponse, PutLicenseResponse> {

    @Override
    public org.elasticsearch.client.license.PutLicenseResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.license.PutLicenseResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.protocol.xpack.license.PutLicenseResponse convertHlrcToInternal(
        org.elasticsearch.client.license.PutLicenseResponse instance) {
        return new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(instance.isAcknowledged(),
            org.elasticsearch.protocol.xpack.license.LicensesStatus.valueOf(instance.status().name()),
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
    protected org.elasticsearch.protocol.xpack.license.PutLicenseResponse createTestInstance() {
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
    protected Writeable.Reader<org.elasticsearch.protocol.xpack.license.PutLicenseResponse> instanceReader() {
        return org.elasticsearch.protocol.xpack.license.PutLicenseResponse::new;
    }

    @Override
    protected org.elasticsearch.protocol.xpack.license.PutLicenseResponse mutateInstance(
        org.elasticsearch.protocol.xpack.license.PutLicenseResponse response) {
        @SuppressWarnings("unchecked")
        Function<org.elasticsearch.protocol.xpack.license.PutLicenseResponse,
            org.elasticsearch.protocol.xpack.license.PutLicenseResponse> mutator = randomFrom(
            r -> new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(
                r.isAcknowledged() == false,
                r.status(),
                r.acknowledgeHeader(),
                r.acknowledgeMessages()),
            r -> new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(
                r.isAcknowledged(),
                mutateStatus(r.status()),
                r.acknowledgeHeader(),
                r.acknowledgeMessages()),
            r -> {
                if (r.acknowledgeMessages().isEmpty()) {
                    return new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(
                        r.isAcknowledged(),
                        r.status(),
                        randomAlphaOfLength(10),
                        randomAckMessages()
                    );
                } else {
                    return new org.elasticsearch.protocol.xpack.license.PutLicenseResponse(r.isAcknowledged(), r.status());
                }
            }

        );
        return mutator.apply(response);
    }

    private org.elasticsearch.protocol.xpack.license.LicensesStatus mutateStatus(
        org.elasticsearch.protocol.xpack.license.LicensesStatus status) {
        return randomValueOtherThan(status, () -> randomFrom(LicensesStatus.values()));
    }
}
