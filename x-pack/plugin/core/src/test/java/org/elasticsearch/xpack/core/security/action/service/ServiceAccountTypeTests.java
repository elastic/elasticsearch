/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountTypeTests extends ESTestCase {

    /**
     * The values are REST contract, so this pins them rather than deriving them from the constants: a rename that
     * changed what the API emits should fail here.
     */
    public void testValues() {
        assertThat(
            Arrays.stream(ServiceAccountType.values()).map(ServiceAccountType::value).toList(),
            equalTo(List.of("built_in", "user_managed"))
        );
    }

    /**
     * The wire form is the ordinal, and {@link ServiceAccountInfo} reads it to decide which kind of account follows,
     * so a reordering would have an older node read one kind as the other rather than fail.
     */
    public void testSerialization() {
        EnumSerializationTestUtils.assertEnumSerialization(
            ServiceAccountType.class,
            ServiceAccountType.BUILT_IN,
            ServiceAccountType.USER_MANAGED
        );
    }

    public void testFromValueAcceptsEveryValue() {
        for (ServiceAccountType type : ServiceAccountType.values()) {
            assertThat(ServiceAccountType.fromValue(type.value()), equalTo(type));
        }
    }

    public void testFromValueRejectsAnythingElseAndSaysWhatItAccepts() {
        for (String value : new String[] { "ELASTIC", "elastic", "user", "users", "", null }) {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServiceAccountType.fromValue(value));
            assertThat(e.getMessage(), equalTo("invalid type value [" + value + "]; must be one of [built_in, user_managed]"));
        }
    }

    public void testToStringIsTheValueSoItReadsTheSameInMessagesAndResponses() {
        for (ServiceAccountType type : ServiceAccountType.values()) {
            assertThat(type.toString(), equalTo(type.value()));
        }
    }
}
