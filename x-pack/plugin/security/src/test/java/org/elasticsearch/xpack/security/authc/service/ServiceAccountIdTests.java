/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountIdTests extends ESTestCase {

    public void testFromPrincipalAndInstantiate() {
        final String namespace1 = randomAlphaOfLengthBetween(3, 8);
        final String serviceName1 = randomAlphaOfLengthBetween(3, 8);
        final String principal1 = namespace1 + "/" + serviceName1;

        final ServiceAccount.ServiceAccountId accountId1;
        if (randomBoolean()) {
            accountId1 = ServiceAccount.ServiceAccountId.fromPrincipal(principal1);
        } else {
            accountId1 = new ServiceAccount.ServiceAccountId(namespace1, serviceName1);
        }
        assertThat(accountId1.asPrincipal(), equalTo(principal1));
        assertThat(accountId1.namespace(), equalTo(namespace1));
        assertThat(accountId1.serviceName(), equalTo(serviceName1));

        // No '/'
        final String principal2 = randomAlphaOfLengthBetween(6, 16);
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> ServiceAccount.ServiceAccountId.fromPrincipal(principal2)
        );
        assertThat(
            e2.getMessage(),
            containsString("a service account ID must be in the form {namespace}/{service-name}, but was [" + principal2 + "]")
        );

        // blank namespace
        final IllegalArgumentException e3;
        if (randomBoolean()) {
            e3 = expectThrows(
                IllegalArgumentException.class,
                () -> ServiceAccount.ServiceAccountId.fromPrincipal(
                    randomFrom("", " ", "\t", " \t") + "/" + randomAlphaOfLengthBetween(3, 8)
                )
            );
        } else {
            e3 = expectThrows(
                IllegalArgumentException.class,
                () -> new ServiceAccount.ServiceAccountId(randomFrom("", " ", "\t", " \t", null), randomAlphaOfLengthBetween(3, 8))
            );
        }
        assertThat(e3.getMessage(), containsString("the namespace of a service account ID must not be empty"));

        // blank service-name
        final IllegalArgumentException e4;
        if (randomBoolean()) {
            e4 = expectThrows(
                IllegalArgumentException.class,
                () -> ServiceAccount.ServiceAccountId.fromPrincipal(
                    randomAlphaOfLengthBetween(3, 8) + "/" + randomFrom("", " ", "\t", " \t")
                )
            );
        } else {
            e4 = expectThrows(
                IllegalArgumentException.class,
                () -> new ServiceAccount.ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomFrom("", " ", "\t", " \t", null))
            );
        }
        assertThat(e4.getMessage(), containsString("the service-name of a service account ID must not be empty"));
    }

    public void testStreamReadWrite() throws IOException {
        final ServiceAccount.ServiceAccountId accountId = new ServiceAccount.ServiceAccountId(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            accountId.write(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(new ServiceAccount.ServiceAccountId(in), equalTo(accountId));
            }
        }
    }
}
