/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UserTests extends ESTestCase {

    public void testWriteToAndReadFrom() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30),
                generateRandomStringArray(20, 30, false));
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
        User user = new User(randomAlphaOfLengthBetween(4, 30),
            randomBoolean() ? generateRandomStringArray(20, 30, false) : null,
            authUser);

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

    public void testRunAsBackcompatRead() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30),
            randomBoolean() ? generateRandomStringArray(20, 30, false) : null);
        // store the runAs user as the "authenticationUser" here to mimic old format for writing
        User authUser = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false), user);

        BytesStreamOutput output = new BytesStreamOutput();
        User.writeTo(authUser, output);
        StreamInput input = output.bytes().streamInput();
        input.setVersion(randomFrom(Version.V_5_0_0, Version.V_5_4_0));
        User readFrom = User.readFrom(input);

        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        User readFromAuthUser = readFrom.authenticatedUser();
        assertThat(authUser, is(notNullValue()));
        assertThat(readFromAuthUser.principal(), is(authUser.principal()));
        assertThat(Arrays.equals(readFromAuthUser.roles(), authUser.roles()), is(true));
    }

    public void testRunAsBackcompatWrite() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30),
            randomBoolean() ? generateRandomStringArray(20, 30, false) : null);
        // store the runAs user as the "authenticationUser" here to mimic old format for writing
        User authUser = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false), user);

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(randomFrom(Version.V_5_0_0, Version.V_5_4_0));
        User.writeTo(authUser, output);
        StreamInput input = output.bytes().streamInput();
        User readFrom = User.readFrom(input);

        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        User readFromAuthUser = readFrom.authenticatedUser();
        assertThat(authUser, is(notNullValue()));
        assertThat(readFromAuthUser.principal(), is(authUser.principal()));
        assertThat(Arrays.equals(readFromAuthUser.roles(), authUser.roles()), is(true));
    }

    public void testSystemUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(SystemUser.INSTANCE, output);
        User readFrom = User.readFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(SystemUser.INSTANCE)));
        assertThat(readFrom.authenticatedUser(), is(SystemUser.INSTANCE));
    }

    public void testXPackUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(XPackUser.INSTANCE, output);
        User readFrom = User.readFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(XPackUser.INSTANCE)));
        assertThat(readFrom.authenticatedUser(), is(XPackUser.INSTANCE));
    }

    public void testFakeInternalUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(true);
        output.writeString(randomAlphaOfLengthBetween(4, 30));
        try {
            User.readFrom(output.bytes().streamInput());
            fail("system user had wrong name");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    public void testUserToString() throws Exception {
        User user = new User("u1", "r1");
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}]"));
        user = new User("u1", new String[] { "r1", "r2" }, "user1", "user1@domain.com", Collections.singletonMap("key", "val"), true);
        assertThat(user.toString(), is("User[username=u1,roles=[r1,r2],fullName=user1,email=user1@domain.com,metadata={key=val}]"));
        user = new User("u1", new String[] {"r1"}, new User("u2", "r2", "r3"));
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}," +
                "authenticatedUser=[User[username=u2,roles=[r2,r3],fullName=null,email=null,metadata={}]]]"));
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
    }
}
