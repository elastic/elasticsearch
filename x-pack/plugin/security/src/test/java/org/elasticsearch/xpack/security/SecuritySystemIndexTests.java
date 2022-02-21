/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;

import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;

public class SecuritySystemIndexTests extends ESTestCase {

    public void testSystemIndexNameIsRestricted() {
        final SystemIndexDescriptor descriptor = Security.getSecurityMainIndexDescriptor();
        Consumer<String> check = idx -> assertThat(
            "For index [" + idx + "]",
            descriptor.matchesIndexPattern(idx),
            is(RestrictedIndicesNames.isRestricted(idx))
        );

        check.accept(".security-" + randomIntBetween(0, 99));
        check.accept(".security" + randomIntBetween(0, 99));

        check.accept(".security-" + randomAlphaOfLengthBetween(1, 12));
        check.accept(".security" + randomAlphaOfLengthBetween(1, 12));

        check.accept(".security-" + randomIntBetween(0, 99) + (randomBoolean() ? "-" : "") + randomAlphaOfLengthBetween(1, 12));
        check.accept(".security-" + randomAlphaOfLengthBetween(1, 12) + (randomBoolean() ? "-" : "") + randomIntBetween(0, 99));

        check.accept("." + randomAlphaOfLengthBetween(1, 12) + "-security");
    }
}
