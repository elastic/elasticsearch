/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UserTests extends ESTestCase {
    public void testWriteToAndReadFrom() throws Exception {
        User user = new User.Simple(randomAsciiOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false));
        BytesStreamOutput output = new BytesStreamOutput();

        User.writeTo(user, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
        assertThat(readFrom.runAs(), is(nullValue()));
    }

    public void testWriteToAndReadFromWithRunAs() throws Exception {
        User runAs = new User.Simple(randomAsciiOfLengthBetween(4, 30), randomBoolean() ? generateRandomStringArray(20, 30, false) : null);
        User user = new User.Simple(randomAsciiOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false), runAs);
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

        User.writeTo(User.SYSTEM, output);
        User readFrom = User.readFrom(ByteBufferStreamInput.wrap(output.bytes()));

        assertThat(readFrom, is(sameInstance(User.SYSTEM)));
        assertThat(readFrom.principal(), is(User.SYSTEM.principal()));
        assertThat(Arrays.equals(readFrom.roles(), User.SYSTEM.roles()), is(true));
        assertThat(readFrom.runAs(), is(nullValue()));
    }

    public void testFakeSystemUserSerialization() throws Exception {
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
            new User.Simple(randomAsciiOfLengthBetween(3, 10), generateRandomStringArray(16, 30, false), User.SYSTEM);
            fail("should not be able to create a runAs user with the system user");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("system"));
        }
    }
}
