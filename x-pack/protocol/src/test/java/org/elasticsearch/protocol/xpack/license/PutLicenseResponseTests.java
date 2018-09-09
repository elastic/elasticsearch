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
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class PutLicenseResponseTests extends AbstractStreamableXContentTestCase<PutLicenseResponse> {

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
    protected PutLicenseResponse doParseInstance(XContentParser parser) throws IOException {
        return PutLicenseResponse.fromXContent(parser);
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
