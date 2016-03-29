/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.pki.PkiRealm;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for the AuthenticationModule
 */
public class AuthenticationModuleTests extends ESTestCase {
    public void testAddingReservedRealmType() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomFrom(PkiRealm.TYPE, LdapRealm.TYPE, ActiveDirectoryRealm.TYPE, FileRealm.TYPE),
                    randomFrom(PkiRealm.Factory.class, LdapRealm.Factory.class, ActiveDirectoryRealm.Factory.class,
                            FileRealm.Factory.class));
            fail("overriding a built in realm type is not allowed!");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("cannot redefine"));
        }
    }

    public void testAddingNullOrEmptyType() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomBoolean() ? null : "",
                    randomFrom(PkiRealm.Factory.class, LdapRealm.Factory.class, ActiveDirectoryRealm.Factory.class,
                            FileRealm.Factory.class));
            fail("type must not be null");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("null or empty"));
        }
    }

    public void testAddingNullFactory() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomAsciiOfLength(7), null);
            fail("factory must not be null");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("null"));
        }
    }
}
