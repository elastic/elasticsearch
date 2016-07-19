/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authz.store.RolesStore;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityFeatureSetTests extends ESTestCase {

    private Settings settings;
    private SecurityLicenseState licenseState;
    private Realms realms;
    private NamedWriteableRegistry namedWriteableRegistry;
    private IPFilter ipFilter;
    private RolesStore rolesStore;
    private AuditTrailService auditTrail;
    private CryptoService cryptoService;

    @Before
    public void init() throws Exception {
        settings = Settings.builder().put("path.home", createTempDir()).build();
        licenseState = mock(SecurityLicenseState.class);
        realms = mock(Realms.class);
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
        ipFilter = mock(IPFilter.class);
        rolesStore = mock(RolesStore.class);
        auditTrail = mock(AuditTrailService.class);
        cryptoService = mock(CryptoService.class);
    }

    public void testWritableRegistration() throws Exception {
        new SecurityFeatureSet(settings, licenseState, realms, namedWriteableRegistry, rolesStore, ipFilter, auditTrail, cryptoService);
        verify(namedWriteableRegistry).register(eq(SecurityFeatureSet.Usage.class), eq("xpack.usage.security"), anyObject());
    }

    public void testAvailable() throws Exception {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms, namedWriteableRegistry, rolesStore,
                ipFilter, auditTrail, cryptoService);
        boolean available = randomBoolean();
        when(licenseState.authenticationAndAuthorizationEnabled()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledSetting() throws Exception {
        boolean enabled = randomBoolean();
        Settings settings = Settings.builder()
                .put(this.settings)
                .put("xpack.security.enabled", enabled)
                .build();
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms, namedWriteableRegistry, rolesStore,
                ipFilter, auditTrail, cryptoService);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testEnabledDefault() throws Exception {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms, namedWriteableRegistry, rolesStore,
                        ipFilter, auditTrail, cryptoService);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testSystemKeyUsageEnabledByCryptoService() {
        final boolean enabled = randomBoolean();

        when(cryptoService.isEncryptionEnabled()).thenReturn(enabled);

        assertThat(SecurityFeatureSet.systemKeyUsage(cryptoService), is(enabled));
    }

    public void testSystemKeyUsageNotEnabledIfNull() {
        assertThat(SecurityFeatureSet.systemKeyUsage(null), is(false));
    }

    public void testUsage() throws Exception {

        boolean authcAuthzAvailable = randomBoolean();
        when(licenseState.authenticationAndAuthorizationEnabled()).thenReturn(authcAuthzAvailable);

        Settings.Builder settings = Settings.builder().put(this.settings);

        boolean enabled = randomBoolean();
        settings.put("xpack.security.enabled", enabled);

        final boolean httpSSLEnabled = randomBoolean();
        settings.put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), httpSSLEnabled);
        final boolean transportSSLEnabled = randomBoolean();
        settings.put(SecurityNetty3Transport.SSL_SETTING.getKey(), transportSSLEnabled);
        final boolean auditingEnabled = randomBoolean();
        final String[] auditOutputs = randomFrom(new String[] {"logfile"}, new String[] {"index"}, new String[] {"logfile", "index"});
        when(auditTrail.usageStats())
                .thenReturn(MapBuilder.<String, Object>newMapBuilder()
                        .put("enabled", auditingEnabled)
                        .put("outputs", auditOutputs)
                        .map());

        final boolean httpIpFilterEnabled = randomBoolean();
        final boolean transportIPFilterEnabled = randomBoolean();
        when(ipFilter.usageStats())
                .thenReturn(MapBuilder.<String, Object>newMapBuilder()
                        .put("http", Collections.singletonMap("enabled", httpIpFilterEnabled))
                        .put("transport", Collections.singletonMap("enabled", transportIPFilterEnabled))
                        .map());


        final boolean rolesStoreEnabled = randomBoolean();
        if (rolesStoreEnabled) {
            when(rolesStore.usageStats()).thenReturn(Collections.singletonMap("count", 1));
        } else {
            when(rolesStore.usageStats()).thenReturn(Collections.emptyMap());
        }
        final boolean useSystemKey = randomBoolean();
        when(cryptoService.isEncryptionEnabled()).thenReturn(useSystemKey);

        List<Realm> realmsList= new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Realm realm = mock(Realm.class);
            when(realm.type()).thenReturn("type" + i);
            realmsList.add(realm);
            Map<String, Object> realmUsage = new HashMap<>();
            realmUsage.put("key1", "value" + i);
            realmUsage.put("key2", i);
            realmUsage.put("key3", i % 2 == 0);
            when(realm.usageStats()).thenReturn(realmUsage);
        }
        when(realms.iterator()).thenReturn(authcAuthzAvailable ? realmsList.iterator() : Collections.<Realm>emptyIterator());

        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings.build(), licenseState, realms, namedWriteableRegistry, rolesStore,
                ipFilter, auditTrail, cryptoService);
        XPackFeatureSet.Usage usage = featureSet.usage();
        assertThat(usage, is(notNullValue()));
        assertThat(usage.name(), is(Security.NAME));
        assertThat(usage.enabled(), is(enabled));
        assertThat(usage.available(), is(authcAuthzAvailable));
        XContentSource source = new XContentSource(usage);

        if (enabled) {
            if (authcAuthzAvailable) {
                for (int i = 0; i < 5; i++) {
                    assertThat(source.getValue("enabled_realms." + i + ".key1"), is("value" + i));
                    assertThat(source.getValue("enabled_realms." + i + ".key2"), is(i));
                    assertThat(source.getValue("enabled_realms." + i + ".key3"), is(i % 2 == 0));
                }
            } else {
                assertThat(source.getValue("enabled_realms"), is(notNullValue()));
            }

            // check SSL
            assertThat(source.getValue("ssl.http.enabled"), is(httpSSLEnabled));
            assertThat(source.getValue("ssl.transport.enabled"), is(transportSSLEnabled));

            // auditing
            assertThat(source.getValue("audit.enabled"), is(auditingEnabled));
            assertThat(source.getValue("audit.outputs"), contains(auditOutputs));

            // ip filter
            assertThat(source.getValue("ipfilter.http.enabled"), is(httpIpFilterEnabled));
            assertThat(source.getValue("ipfilter.transport.enabled"), is(transportIPFilterEnabled));

            // roles
            if (rolesStoreEnabled) {
                assertThat(source.getValue("roles.count"), is(1));
            } else {
                assertThat(((Map) source.getValue("roles")).isEmpty(), is(true));
            }

            // system key
            assertThat(source.getValue("system_key"), is(useSystemKey));
        } else {
            assertThat(source.getValue("enabled_realms"), is(nullValue()));
        }
    }
}
