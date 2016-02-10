/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UserTests extends ESTestCase {

    public void testWriteToAndReadFrom() throws Exception {
        User user = new User(randomAsciiOfLengthBetween(4, 30),
                generateRandomStringArray(20, 30, false));
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(user, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        assertThat(readFrom.runAs(), is(nullValue()));
    }

    public void testWriteToAndReadFromWithRunAs() throws Exception {
        User runAs = new User(randomAsciiOfLengthBetween(4, 30),
                randomBoolean() ? generateRandomStringArray(20, 30, false) : null);
        User user = new User(randomAsciiOfLengthBetween(4, 30),
                generateRandomStringArray(20, 30, false), runAs);
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(user, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        assertThat(readFrom.runAs(), is(notNullValue()));
        User readFromRunAs = readFrom.runAs();
        assertThat(readFromRunAs.principal(), is(runAs.principal()));
        assertThat(Arrays.equals(readFromRunAs.roles(), runAs.roles()), is(true));
        assertThat(readFromRunAs.runAs(), is(nullValue()));
    }

    public void testSystemReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(SystemUser.INSTANCE, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, is(sameInstance(SystemUser.INSTANCE)));
        assertThat(readFrom.runAs(), is(nullValue()));
    }

    public void testFakeInternalUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(true);
        output.writeString(randomAsciiOfLengthBetween(4, 30));
        try {
            User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));
            fail("system user had wrong name");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    public void testCreateUserRunningAsSystemUser() throws Exception {
        try {
            new User(randomAsciiOfLengthBetween(3, 10),
                    generateRandomStringArray(16, 30, false), SystemUser.INSTANCE);
            fail("should not be able to create a runAs user with the system user");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("invalid run_as user"));
        }
    }

    public void testUserToString() throws Exception {
        User user = new User("u1", "r1");
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}]"));
        user = new User("u1", new String[] { "r1", "r2" }, "user1", "user1@domain.com", Collections.singletonMap("key", "val"));
        assertThat(user.toString(), is("User[username=u1,roles=[r1,r2],fullName=user1,email=user1@domain.com,metadata={key=val}]"));
        user = new User("u1", new String[] {"r1", "r2"}, new User("u2", "r3"));
        assertThat(user.toString(), is("User[username=u1,roles=[r1,r2],fullName=null,email=null,metadata={},runAs=[User[username=u2," +
                "roles=[r3],fullName=null,email=null,metadata={}]]]"));
    }

    public void testReservedUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        User.writeTo(XPackUser.INSTANCE, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, is(sameInstance(XPackUser.INSTANCE)));

        output = new BytesStreamOutput();
        User.writeTo(KibanaUser.INSTANCE, output);
        readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, is(sameInstance(KibanaUser.INSTANCE)));
    }

    public void testReservedMetadata() throws Exception {
        Map<String, Object> validMetadata = Collections.singletonMap("foo", "bar");
        Map<String, Object> invalidMetadata = Collections.singletonMap(User.RESERVED_PREFIX + "foo", "bar");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                new User("john", Strings.EMPTY_ARRAY, "John Doe", "john@doe.com", invalidMetadata));
        assertThat(exception.getMessage(), containsString("reserved"));

        User user = new User("john", Strings.EMPTY_ARRAY, "John Doe", "john@doe.com", validMetadata);
        assertNotNull(user);
    }
}
