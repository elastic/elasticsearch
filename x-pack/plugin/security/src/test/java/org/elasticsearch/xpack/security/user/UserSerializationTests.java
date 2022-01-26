/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.InternalUserSerializationHelper;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UserSerializationTests extends ESTestCase {

    public void testWriteToAndReadFrom() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false));
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(user, output);
        User readFrom = User.readFrom(output.bytes().streamInput());

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        assertThat(readFrom.authenticatedUser(), is(user));
    }

    public void testWriteToAndReadFromWithRunAs() throws Exception {
        User authUser = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false));
        User user = new User(
            randomAlphaOfLengthBetween(4, 30),
            randomBoolean() ? generateRandomStringArray(20, 30, false) : null,
            authUser
        );

        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(user, output);
        User readFrom = User.readFrom(output.bytes().streamInput());

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        User readFromAuthUser = readFrom.authenticatedUser();
        assertThat(authUser, is(notNullValue()));
        assertThat(readFromAuthUser.principal(), is(authUser.principal()));
        assertThat(Arrays.equals(readFromAuthUser.roles(), authUser.roles()), is(true));
        assertThat(readFromAuthUser.authenticatedUser(), is(authUser));
    }

    public void testSystemUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        InternalUserSerializationHelper.writeTo(SystemUser.INSTANCE, output);
        User readFrom = InternalUserSerializationHelper.readFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(SystemUser.INSTANCE)));
        assertThat(readFrom.authenticatedUser(), is(SystemUser.INSTANCE));
    }

    public void testSystemUserFailsRead() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        InternalUserSerializationHelper.writeTo(SystemUser.INSTANCE, output);
        AssertionError e = expectThrows(AssertionError.class, () -> User.readFrom(output.bytes().streamInput()));

        assertThat(e.getMessage(), is("should always return false. Internal users should use the InternalUserSerializationHelper"));
    }

    public void testXPackUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        InternalUserSerializationHelper.writeTo(XPackUser.INSTANCE, output);
        User readFrom = InternalUserSerializationHelper.readFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(XPackUser.INSTANCE)));
        assertThat(readFrom.authenticatedUser(), is(XPackUser.INSTANCE));
    }

    public void testAsyncSearchUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        InternalUserSerializationHelper.writeTo(AsyncSearchUser.INSTANCE, output);
        User readFrom = InternalUserSerializationHelper.readFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(AsyncSearchUser.INSTANCE)));
        assertThat(readFrom.authenticatedUser(), is(AsyncSearchUser.INSTANCE));
    }

    public void testFakeInternalUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(true);
        output.writeString(randomAlphaOfLengthBetween(4, 30));
        try {
            InternalUserSerializationHelper.readFrom(output.bytes().streamInput());
            fail("system user had wrong name");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    public void testReservedUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        final ElasticUser elasticUser = new ElasticUser(true);
        User.writeTo(elasticUser, output);
        User readFrom = User.readFrom(output.bytes().streamInput());

        assertEquals(elasticUser, readFrom);

        final KibanaUser kibanaUser = new KibanaUser(true);
        output = new BytesStreamOutput();
        User.writeTo(kibanaUser, output);
        readFrom = User.readFrom(output.bytes().streamInput());

        assertEquals(kibanaUser, readFrom);

        final KibanaSystemUser kibanaSystemUser = new KibanaSystemUser(true);
        output = new BytesStreamOutput();
        User.writeTo(kibanaSystemUser, output);
        readFrom = User.readFrom(output.bytes().streamInput());

        assertEquals(kibanaSystemUser, readFrom);
    }
}
