/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ClientEnrollmentRequestTests extends ESTestCase {

    public void test() throws Exception{
        final String clientType = randomFrom("kibana", "generic_client");
        final boolean withPassword = clientType.equals("kibana") && randomBoolean();
        final char[] password = randomAlphaOfLengthBetween(8, 16).toCharArray();
        ClientEnrollmentRequest request = new ClientEnrollmentRequest(clientType, withPassword ? password : null);

        Map<String, Object> expected = new HashMap<>(Map.of(
            "client_type", clientType
        ));
        if (withPassword) {
            expected.put("client_password", new String(password));
        }

        assertThat(XContentHelper.convertToMap(XContentHelper.toXContent(
            request, XContentType.JSON, false), false, XContentType.JSON).v2(), equalTo(expected));
    }

    public void testEqualsHashCode() {
        final String clientType = randomFrom("kibana", "generic_client");
        final boolean withPassword = clientType.equals("kibana") && randomBoolean();
        final char[] password = randomAlphaOfLengthBetween(8, 16).toCharArray();
        ClientEnrollmentRequest request = new ClientEnrollmentRequest(clientType, withPassword ? password : null);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request, (original) -> {
            return new ClientEnrollmentRequest(original.getClientType(), original.getClientPassword());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request, (original) -> {
            return new ClientEnrollmentRequest(original.getClientType(), original.getClientPassword());
        }, ClientEnrollmentRequestTests::mutateTestItem);
    }

    private static ClientEnrollmentRequest mutateTestItem(ClientEnrollmentRequest original) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new ClientEnrollmentRequest(original.getClientType(),
                    original.getClientPassword() == null ? "somepassword".toCharArray() : null);
            case 1:
                return new ClientEnrollmentRequest(randomValueOtherThan(
                    original.getClientType(),
                    () -> randomFrom("kibana", "generic_client")), original.getClientPassword());
            case 2:
                return new ClientEnrollmentRequest(original.getClientType(),
                    original.getClientPassword() == null ? "somepassword".toCharArray() :
                        (new String(original.getClientPassword()) + randomAlphaOfLength(4)).toCharArray());
            default:
                return new ClientEnrollmentRequest(randomAlphaOfLength(10), original.getClientPassword());
        }

    }
}
