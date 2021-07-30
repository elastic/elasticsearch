/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class InternalRealmsTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testNativeRealmRegistersIndexHealthChangeListener() throws Exception {
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        Map<String, Realm.Factory> factories = InternalRealms.getFactories(mock(ThreadPool.class), mock(ResourceWatcherService.class),
                mock(SSLService.class), mock(NativeUsersStore.class), mock(NativeRoleMappingStore.class), securityIndex);
        assertThat(factories, hasEntry(is(NativeRealmSettings.TYPE), any(Realm.Factory.class)));
        verifyZeroInteractions(securityIndex);

        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, "test");
        Settings settings = Settings.builder().put("path.home", createTempDir())
            .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final ThreadContext threadContext = new ThreadContext(settings);
        factories.get(NativeRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
        verify(securityIndex).addStateListener(isA(BiConsumer.class));

        factories.get(NativeRealmSettings.TYPE).create(new RealmConfig(realmId, settings, env, threadContext));
        verify(securityIndex, times(2)).addStateListener(isA(BiConsumer.class));
    }

    public void testIsStandardType() {
        String type = randomFrom(NativeRealmSettings.TYPE, FileRealmSettings.TYPE, LdapRealmSettings.AD_TYPE, LdapRealmSettings.LDAP_TYPE,
                PkiRealmSettings.TYPE);
        assertThat(InternalRealms.isStandardRealm(type), is(true));
        type = randomFrom(SamlRealmSettings.TYPE, KerberosRealmSettings.TYPE, OpenIdConnectRealmSettings.TYPE);
        assertThat(InternalRealms.isStandardRealm(type), is(false));
    }
}
