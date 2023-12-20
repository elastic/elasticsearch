/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.ParentActionAuthorization;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ParentActionAuthorization} class.
 */
public class ParentActionAuthorizationTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ParentActionAuthorization authorization = createRandom();
        final BytesStreamOutput out = new BytesStreamOutput();
        authorization.writeTo(out);
        assertThat(ParentActionAuthorization.readFrom(out.bytes().streamInput()), equalTo(authorization));
    }

    private static ParentActionAuthorization createRandom() {
        String action = randomAlphaOfLengthBetween(5, 20);
        return new ParentActionAuthorization(action);
    }

}
