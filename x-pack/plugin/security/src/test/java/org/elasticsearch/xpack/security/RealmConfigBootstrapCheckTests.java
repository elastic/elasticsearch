/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;;

public class RealmConfigBootstrapCheckTests extends ESTestCase {
    private Settings.Builder builder;

    @Before
    public void setup() {
        builder = Settings.builder();
        builder.put("path.home", createTempDir());
        builder.put("node.name", randomAlphaOfLength(5));
    }

    public void testRealmsNoErrors() throws Exception {
        withRealm(PkiRealmSettings.TYPE, randomInt(5), builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        assertThat(runCheck(builder.build()).isSuccess(), is(true));
    }

    public void testRealmsWithMultipleInternalRealmsConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(FileRealmSettings.TYPE, 2, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo("multiple [" + FileRealmSettings.TYPE + "] realms are configured. ["
                + FileRealmSettings.TYPE + "] is an internal realm and therefore there can only be one such realm configured"));
    }

    public void testRealmsWithEmptyTypeConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        final String name = randomAlphaOfLength(4);
        builder.put("xpack.security.authc.realms." + name + ".type", "");
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo("missing realm type for [" + name + "] realm"));
    }

    public void testLookupAwareRealmDependingOnLookupRealms() throws Exception {
        withRealm(FileRealmSettings.TYPE, 1, builder);
        final String ldapRealmName = randomAlphaOfLength(5);
        withRealm(LdapRealmSettings.LDAP_TYPE, ldapRealmName, builder);
        final String dummyLookupAwareRealmName = "dlrar-" + randomAlphaOfLength(5);
        withRealm(DummySecurityExtension.DLRAR_TYPE, dummyLookupAwareRealmName, builder, new String[] { ldapRealmName });
        final String pkiRealmName = "pki-" + randomAlphaOfLength(5);
        withRealm(PkiRealmSettings.TYPE, pkiRealmName, builder, new String[] { ldapRealmName });
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());

        assertThat(result.isSuccess(), is(true));
    }

    public void testLookupAwareRealmDependingOnNonExistingOrDisabledRealms() throws Exception {
        withRealm(FileRealmSettings.TYPE, 1, builder);
        final String ldapRealmName = randomAlphaOfLength(5);
        final boolean nonexisting = randomBoolean();
        if (nonexisting == false) {
            withRealm(LdapRealmSettings.LDAP_TYPE, ldapRealmName, builder, false);
        }
        final String pkiRealmName = "pki-" + randomAlphaOfLength(5);
        withRealm(PkiRealmSettings.TYPE, pkiRealmName, builder, new String[] { ldapRealmName });
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());

        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo(ldapRealmName + " used as lookup realm does not exist or is disabled"));
    }

    public void testLookupAwareRealmDependingAnotherLookupAwareRealm() throws Exception {
        withRealm(FileRealmSettings.TYPE, 1, builder);
        final String ldapRealmName = randomAlphaOfLength(5);
        withRealm(LdapRealmSettings.LDAP_TYPE, ldapRealmName, builder);
        final String dummyLookupAwareRealmName = "dlrar-" + randomAlphaOfLength(5);
        withRealm(DummySecurityExtension.DLRAR_TYPE, dummyLookupAwareRealmName, builder, new String[] { ldapRealmName });
        final String pkiRealmName = "pki-" + randomAlphaOfLength(5);
        withRealm(PkiRealmSettings.TYPE, pkiRealmName, builder, new String[] { dummyLookupAwareRealmName });
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());

        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(),
                equalTo(dummyLookupAwareRealmName + " used as lookup realm is itself dependent on other realms for lookup"));
    }

    public void testRealmsWithUnknownTypeConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        final String name = randomAlphaOfLength(4);
        final String type = randomAlphaOfLength(5);
        builder.put("xpack.security.authc.realms." + name + ".type", type);
        final BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo("unknown realm type [" + type + "] set for realm [" + name + "]"));
    }

    private static void withRealm(String type, int instances, Settings.Builder builder) {
        for (int i = 0; i < instances; i++) {
            withRealm(type, randomAlphaOfLength(4), builder);
        }
    }

    private static void withRealm(String type, String realmName, Settings.Builder builder, String... lookupRealms) {
        withRealm(type, realmName, builder, true, lookupRealms);
    }

    private static void withRealm(String type, String realmName, Settings.Builder builder, Boolean enabled, String... lookupRealms) {
        builder.put("xpack.security.authc.realms." + realmName + ".type", type);
        if (lookupRealms != null) {
            builder.put("xpack.security.authc.realms." + realmName + ".lookup_realms", Strings.arrayToCommaDelimitedString(lookupRealms));
        }
        if (enabled != null) {
            builder.put("xpack.security.authc.realms." + realmName + ".enabled", Boolean.toString(enabled));
        }
    }

    private BootstrapCheck.BootstrapCheckResult runCheck(Settings settings) throws Exception {
        final List<SecurityExtension> securityExtensions = new ArrayList<>();
        securityExtensions.add(new DummySecurityExtension());
        return new RealmConfigBootstrapCheck(securityExtensions).check(new BootstrapContext(settings, null));
    }

    private static class DummySecurityExtension implements SecurityExtension {
        static final String DLRAR_TYPE = "DLRAR";

        @Override
        public Map<String, Realm.Factory> getRealms(ResourceWatcherService resourceWatcherService) {
            final Map<String, Realm.Factory> realmsFactory = new HashMap<>();
            realmsFactory.put(DLRAR_TYPE, (config) -> {
                Realm dummyLookupRealmsAwareRealm = mock(Realm.class);
                when(dummyLookupRealmsAwareRealm.type()).thenReturn(DLRAR_TYPE);
                return dummyLookupRealmsAwareRealm;
            });
            return realmsFactory;
        }
    }
}
