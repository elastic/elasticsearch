/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AuthorizationStateTests extends AbstractSerializingTransformTestCase<AuthorizationState> {

    public static AuthorizationState randomAuthorizationState() {
        return new AuthorizationState(
            randomNonNegativeLong(),
            randomFrom(HealthStatus.values()),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 100)
        );
    }

    @Override
    protected Writeable.Reader<AuthorizationState> instanceReader() {
        return AuthorizationState::new;
    }

    @Override
    protected AuthorizationState createTestInstance() {
        return randomAuthorizationState();
    }

    @Override
    protected AuthorizationState mutateInstance(AuthorizationState instance) {
        int statusCount = HealthStatus.values().length;
        assert statusCount > 1;
        return new AuthorizationState(
            instance.getTimestamp().toEpochMilli() + 1,
            HealthStatus.values()[(instance.getStatus().ordinal() + 1) % statusCount],
            instance.getLastAuthError() == null ? randomAlphaOfLengthBetween(1, 100) : null
        );
    }

    @Override
    protected AuthorizationState doParseInstance(XContentParser parser) throws IOException {
        return AuthorizationState.PARSER.apply(parser, null);
    }

    public void testGreen() {
        AuthorizationState authState = AuthorizationState.green();
        assertThat(authState.getStatus(), is(equalTo(HealthStatus.GREEN)));
        assertThat(authState.getLastAuthError(), is(nullValue()));
    }

    public void testRed() {
        Exception e = new Exception("some exception");
        AuthorizationState authState = AuthorizationState.red(e);
        assertThat(authState.getStatus(), is(equalTo(HealthStatus.RED)));
        assertThat(authState.getLastAuthError(), is(equalTo("some exception")));

        authState = AuthorizationState.red(null);
        assertThat(authState.getStatus(), is(equalTo(HealthStatus.RED)));
        assertThat(authState.getLastAuthError(), is(equalTo("unknown exception")));
    }

    public void testIsNullOrGreen() {
        assertThat(AuthorizationState.isNullOrGreen(null), is(true));
        assertThat(AuthorizationState.isNullOrGreen(AuthorizationState.green()), is(true));
        Exception e = new Exception("some exception");
        assertThat(AuthorizationState.isNullOrGreen(AuthorizationState.red(e)), is(false));
    }
}
