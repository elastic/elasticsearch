/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationSerializationHelper;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AuthenticationSerializationTests extends ESTestCase {

    public void testWriteToAndReadFrom() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false));
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(user, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
    }

    public void testWriteToAndReadFromWithRunAs() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder().runAs().build();
        assertThat(authentication.isRunAs(), is(true));

        BytesStreamOutput output = new BytesStreamOutput();
        authentication.writeTo(output);
        final Authentication readFrom = new Authentication(output.bytes().streamInput());
        assertThat(readFrom.isRunAs(), is(true));

        assertThat(readFrom, not(sameInstance(authentication)));
        final User readFromEffectiveUser = readFrom.getEffectiveSubject().getUser();
        assertThat(readFromEffectiveUser, not(sameInstance(authentication.getEffectiveSubject().getUser())));
        assertThat(readFromEffectiveUser, equalTo(authentication.getEffectiveSubject().getUser()));

        User readFromAuthenticatingUser = readFrom.getAuthenticatingSubject().getUser();
        assertThat(readFromAuthenticatingUser, is(notNullValue()));
        assertThat(readFromAuthenticatingUser, not(sameInstance(authentication.getAuthenticatingSubject().getUser())));
        assertThat(readFromAuthenticatingUser, equalTo(authentication.getAuthenticatingSubject().getUser()));
    }

    public void testWriteToAndReadFromWithRemoteAccess() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder().remoteAccess().build();
        assertThat(authentication.isRemoteAccess(), is(true));

        BytesStreamOutput output = new BytesStreamOutput();
        authentication.writeTo(output);
        final Authentication readFrom = new Authentication(output.bytes().streamInput());
        assertThat(readFrom.isRemoteAccess(), is(true));

        assertThat(readFrom, not(sameInstance(authentication)));
        assertThat(readFrom, equalTo(authentication));
    }

    public void testWriteToWithRemoteAccessThrowsOnUnsupportedVersion() throws Exception {
        final Authentication authentication = randomBoolean()
            ? AuthenticationTestHelper.builder().remoteAccess().build()
            : AuthenticationTestHelper.builder().build();

        final BytesStreamOutput out = new BytesStreamOutput();
        final TransportVersion version = TransportVersionUtils.randomPreviousCompatibleVersion(
            random(),
            Authentication.VERSION_REMOTE_ACCESS_REALM
        );
        out.setTransportVersion(version);

        if (authentication.isRemoteAccess()) {
            final var ex = expectThrows(IllegalArgumentException.class, () -> authentication.writeTo(out));
            assertThat(
                ex.getMessage(),
                containsString(
                    "versions of Elasticsearch before ["
                        + Authentication.VERSION_REMOTE_ACCESS_REALM
                        + "] can't handle remote access authentication and attempted to send to ["
                        + out.getTransportVersion()
                        + "]"
                )
            );
        } else {
            authentication.writeTo(out);
            final StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(out.getTransportVersion());
            final Authentication readFrom = new Authentication(in);
            assertThat(readFrom, equalTo(authentication.maybeRewriteForOlderVersion(out.getTransportVersion())));
        }
    }

    public void testSystemUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(SystemUser.INSTANCE, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(SystemUser.INSTANCE)));
    }

    public void testXPackUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(XPackUser.INSTANCE, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(XPackUser.INSTANCE)));
    }

    public void testAsyncSearchUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(AsyncSearchUser.INSTANCE, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(AsyncSearchUser.INSTANCE)));
    }

    public void testFakeInternalUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(true);
        output.writeString(randomAlphaOfLengthBetween(4, 30));
        try {
            AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());
            fail("system user had wrong name");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    public void testReservedUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        final ElasticUser elasticUser = new ElasticUser(true);
        AuthenticationSerializationHelper.writeUserTo(elasticUser, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(elasticUser, readFrom);

        final KibanaUser kibanaUser = new KibanaUser(true);
        output = new BytesStreamOutput();
        AuthenticationSerializationHelper.writeUserTo(kibanaUser, output);
        readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(kibanaUser, readFrom);

        final KibanaSystemUser kibanaSystemUser = new KibanaSystemUser(true);
        output = new BytesStreamOutput();
        AuthenticationSerializationHelper.writeUserTo(kibanaSystemUser, output);
        readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(kibanaSystemUser, readFrom);
    }
}
