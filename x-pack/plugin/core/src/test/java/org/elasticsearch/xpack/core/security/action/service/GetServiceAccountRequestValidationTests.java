/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetServiceAccountRequestValidationTests extends ESTestCase {

    /**
     * The namespace and service name select which accounts to report rather than name one to look up, so a value no
     * account could carry is answered with no accounts rather than rejected. Every kind of unusable name is asserted
     * here, because each is a name some other service account API does reject.
     */
    public void testNamesAreNeverRejected() {
        for (String namespace : new String[] { null, "elastic", "ELASTIC", "my-team", "my*team", "", "_leading-underscore" }) {
            for (String serviceName : new String[] { null, "fleet-server", "worker*", "", "_leading-underscore" }) {
                assertThat(
                    "namespace [" + namespace + "] and service name [" + serviceName + "] should not be rejected",
                    new GetServiceAccountRequest(namespace, serviceName, randomType()).validate(),
                    nullValue()
                );
            }
        }
    }

    /**
     * A request that names no kind of account could only ever report nothing, so it is a mistake rather than an empty
     * filter.
     */
    public void testEmptyTypeIsRejected() {
        final ActionRequestValidationException e = new GetServiceAccountRequest(
            randomFrom("my-team", null),
            randomFrom("worker", null),
            EnumSet.noneOf(ServiceAccountType.class)
        ).validate();
        assertThat(e, notNullValue());
        assertThat(e.validationErrors(), contains("type must name at least one of [built_in, user_managed]"));
    }

    private static EnumSet<ServiceAccountType> randomType() {
        return randomFrom(
            EnumSet.of(ServiceAccountType.BUILT_IN),
            EnumSet.of(ServiceAccountType.USER_MANAGED),
            EnumSet.allOf(ServiceAccountType.class)
        );
    }
}
