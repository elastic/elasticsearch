/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AnonymousUserTests extends ESTestCase {

    public void testResolveAnonymousUser() throws Exception {
        Settings settings = Settings.builder()
                .put(AnonymousUser.USERNAME_SETTING.getKey(), "anonym1")
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser user = new AnonymousUser(settings);
        assertThat(user.principal(), equalTo("anonym1"));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));

        settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        user = new AnonymousUser(settings);
        assertThat(user.principal(), equalTo(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    public void testResolveAnonymousUser_NoSettings() throws Exception {
        Settings settings = randomBoolean() ?
                Settings.EMPTY :
                Settings.builder().put(AnonymousUser.USERNAME_SETTING.getKey(), "user1").build();
        assertThat(AnonymousUser.isAnonymousEnabled(settings), is(false));
    }

    public void testAnonymous() throws Exception {
        Settings settings = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3").build();
        if (randomBoolean()) {
            settings = Settings.builder().put(settings).put(AnonymousUser.USERNAME_SETTING.getKey(), "anon").build();
        }

        AnonymousUser user = new AnonymousUser(settings);
        assertEquals(user, new AnonymousUser(settings));
        assertThat(AnonymousUser.isAnonymousUsername(user.principal(), settings), is(true));
        // make sure check works with serialization
        BytesStreamOutput output = new BytesStreamOutput();
        User.writeTo(user, output);

        User anonymousSerialized = User.readFrom(output.bytes().streamInput());
        assertEquals(user, anonymousSerialized);

        // test with anonymous disabled
        if (user.principal().equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME)) {
            assertThat(AnonymousUser.isAnonymousUsername(user.principal(), Settings.EMPTY), is(true));
        } else {
            assertThat(AnonymousUser.isAnonymousUsername(user.principal(), Settings.EMPTY), is(false));
        }
    }
}
