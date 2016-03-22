/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AnonymousUserHolderTests extends ESTestCase {
    public void testResolveAnonymousUser() throws Exception {
        Settings settings = Settings.builder()
                .put(AnonymousService.USERNAME_SETTING.getKey(), "anonym1")
                .putArray(AnonymousService.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        User user = AnonymousService.resolveAnonymousUser(settings);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("anonym1"));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));

        settings = Settings.builder()
                .putArray(AnonymousService.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        user = AnonymousService.resolveAnonymousUser(settings);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(AnonymousService.ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    public void testResolveAnonymousUser_NoSettings() throws Exception {
        Settings settings = randomBoolean() ?
                Settings.EMPTY :
                Settings.builder().put(AnonymousService.USERNAME_SETTING.getKey(), "user1").build();
        User user = AnonymousService.resolveAnonymousUser(settings);
        assertThat(user, nullValue());
    }

    public void testWhenAnonymousDisabled() {
        AnonymousService anonymousService = new AnonymousService(Settings.EMPTY);
        assertThat(anonymousService.enabled(), is(false));
        assertThat(anonymousService.isAnonymous(new User(randomAsciiOfLength(10), randomAsciiOfLength(5))), is(false));
        assertThat(anonymousService.anonymousUser(), nullValue());
        assertThat(anonymousService.authorizationExceptionsEnabled(), is(true));
    }

    public void testWhenAnonymousEnabled() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousService.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousService anonymousService = new AnonymousService(settings);
        assertThat(anonymousService.enabled(), is(true));
        assertThat(anonymousService.anonymousUser(), notNullValue());
        assertThat(anonymousService.isAnonymous(anonymousService.anonymousUser()), is(true));
        assertThat(anonymousService.authorizationExceptionsEnabled(), is(true));

        // make sure check works with serialization
        BytesStreamOutput output = new BytesStreamOutput();
        User.writeTo(anonymousService.anonymousUser(), output);
        User anonymousSerialized = User.readFrom(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytes())));
        assertThat(anonymousService.isAnonymous(anonymousSerialized), is(true));
    }

    public void testDisablingAuthorizationExceptions() {
        Settings settings = Settings.builder()
                .putArray(AnonymousService.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .put(AnonymousService.SETTING_AUTHORIZATION_EXCEPTION_ENABLED.getKey(), false)
                .build();
        AnonymousService holder = new AnonymousService(settings);
        assertThat(holder.authorizationExceptionsEnabled(), is(false));
    }
}
