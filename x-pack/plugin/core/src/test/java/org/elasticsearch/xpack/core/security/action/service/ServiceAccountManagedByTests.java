/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountManagedByTests extends ESTestCase {

    /**
     * The values are REST contract, so this pins them rather than deriving them from the constants: a rename that
     * changed what the API emits should fail here.
     */
    public void testValues() {
        assertThat(
            Arrays.stream(ServiceAccountManagedBy.values()).map(ServiceAccountManagedBy::value).toList(),
            equalTo(List.of("elastic", "user"))
        );
    }

    public void testFromValueAcceptsEveryValue() {
        for (ServiceAccountManagedBy managedBy : ServiceAccountManagedBy.values()) {
            assertThat(ServiceAccountManagedBy.fromValue(managedBy.value()), equalTo(managedBy));
        }
    }

    public void testFromValueRejectsAnythingElseAndSaysWhatItAccepts() {
        for (String value : new String[] { "ELASTIC", "built_in", "users", "", null }) {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServiceAccountManagedBy.fromValue(value));
            assertThat(e.getMessage(), equalTo("invalid managed_by value [" + value + "]; must be one of [elastic, user]"));
        }
    }

    public void testToStringIsTheValueSoItReadsTheSameInMessagesAndResponses() {
        for (ServiceAccountManagedBy managedBy : ServiceAccountManagedBy.values()) {
            assertThat(managedBy.toString(), equalTo(managedBy.value()));
        }
    }
}
