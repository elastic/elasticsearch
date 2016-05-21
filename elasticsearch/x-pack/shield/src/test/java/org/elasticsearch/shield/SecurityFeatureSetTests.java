/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SecurityFeatureSetTests extends ESTestCase {

    private SecurityLicenseState licenseState;
    private Realms realms;
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void init() throws Exception {
        licenseState = mock(SecurityLicenseState.class);
        realms = mock(Realms.class);
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
    }

    public void testWritableRegistration() throws Exception {
        new SecurityFeatureSet(Settings.EMPTY, licenseState, realms, namedWriteableRegistry);
        verify(namedWriteableRegistry).register(eq(SecurityFeatureSet.Usage.class), eq("xpack.usage.security"), anyObject());
    }

    public void testAvailable() throws Exception {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(Settings.EMPTY, licenseState, realms, namedWriteableRegistry);
        boolean available = randomBoolean();
        when(licenseState.authenticationAndAuthorizationEnabled()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledSetting() throws Exception {
        boolean enabled = randomBoolean();
        Settings settings = Settings.builder()
                .put("xpack.security.enabled", enabled)
                .build();
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms, namedWriteableRegistry);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testEnabledDefault() throws Exception {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(Settings.EMPTY, licenseState, realms, namedWriteableRegistry);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testUsage() throws Exception {

        boolean available = randomBoolean();
        when(licenseState.authenticationAndAuthorizationEnabled()).thenReturn(available);

        Settings.Builder settings = Settings.builder();

        boolean enabled = randomBoolean();
        settings.put("xpack.security.enabled", enabled);

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
        when(realms.iterator()).thenReturn(realmsList.iterator());

        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings.build(), licenseState, realms, namedWriteableRegistry);
        XPackFeatureSet.Usage usage = featureSet.usage();
        assertThat(usage, is(notNullValue()));
        assertThat(usage.name(), is(Security.NAME));
        assertThat(usage.enabled(), is(enabled));
        assertThat(usage.available(), is(available));
        XContentSource source = new XContentSource(usage);

        if (enabled) {
            for (int i = 0; i < 5; i++) {
                assertThat(source.getValue("enabled_realms." + i + ".key1"), is("value" + i));
                assertThat(source.getValue("enabled_realms." + i + ".key2"), is(i));
                assertThat(source.getValue("enabled_realms." + i + ".key3"), is(i % 2 == 0));
            }
        } else {
            assertThat(source.getValue("enabled_realms"), is(nullValue()));
        }
    }
}
