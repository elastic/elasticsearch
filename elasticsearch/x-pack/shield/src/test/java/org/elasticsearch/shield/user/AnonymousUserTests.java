/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AnonymousUserTests extends ESTestCase {

    @After
    public void resetAnonymous() {
        AnonymousUser.initialize(Settings.EMPTY);
    }

    public void testResolveAnonymousUser() throws Exception {
        Settings settings = Settings.builder()
                .put(AnonymousUser.USERNAME_SETTING.getKey(), "anonym1")
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        User user = AnonymousUser.INSTANCE;
        assertThat(user.principal(), equalTo("anonym1"));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));

        settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        user = AnonymousUser.INSTANCE;
        assertThat(user.principal(), equalTo(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    public void testResolveAnonymousUser_NoSettings() throws Exception {
        Settings settings = randomBoolean() ?
                Settings.EMPTY :
                Settings.builder().put(AnonymousUser.USERNAME_SETTING.getKey(), "user1").build();
        AnonymousUser.initialize(settings);
        assertThat(AnonymousUser.enabled(), is(false));
    }

    public void testAnonymous() throws Exception {
        Settings settings = Settings.builder().putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3").build();
        if (randomBoolean()) {
            settings = Settings.builder().put(settings).put(AnonymousUser.USERNAME_SETTING.getKey(), "anon").build();
        }

        AnonymousUser.initialize(settings);
        User user = AnonymousUser.INSTANCE;
        assertThat(AnonymousUser.is(user), is(true));
        assertThat(AnonymousUser.isAnonymousUsername(user.principal()), is(true));
        // make sure check works with serialization
        BytesStreamOutput output = new BytesStreamOutput();
        User.writeTo(user, output);

        User anonymousSerialized = User.readFrom(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytes())));
        assertThat(AnonymousUser.is(anonymousSerialized), is(true));

        // test with null anonymous
        AnonymousUser.initialize(Settings.EMPTY);
        assertThat(AnonymousUser.is(null), is(false));
        if (user.principal().equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME)) {
            assertThat(AnonymousUser.isAnonymousUsername(user.principal()), is(true));
        } else {
            assertThat(AnonymousUser.isAnonymousUsername(user.principal()), is(false));
        }
    }
}
