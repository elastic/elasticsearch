/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;

public class UserTokenTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final Authentication authentication = new Authentication(new User("joe", "a role"), new RealmRef("realm", "native", "node1"), null);
        final int seconds = randomIntBetween(0, Math.toIntExact(TimeValue.timeValueMinutes(30L).getSeconds()));
        final Instant expirationTime = Clock.systemUTC().instant().plusSeconds(seconds);
        final UserToken userToken = new UserToken(authentication, expirationTime);

        BytesStreamOutput output = new BytesStreamOutput();
        userToken.writeTo(output);

        final UserToken serialized = new UserToken(output.bytes().streamInput());
        assertEquals(authentication, serialized.getAuthentication());
        assertEquals(expirationTime, serialized.getExpirationTime());
    }
}
