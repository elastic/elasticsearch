/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RealmsTests extends ESTestCase {
    private Map<String, Realm.Factory> factories;
    private MockLicenseState licenseState;
    private ThreadContext threadContext;
    private ReservedRealm reservedRealm;
    private int randomRealmTypesCount;
    private List<LicenseStateListener> licenseStateListeners;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();
        factories.put(FileRealmSettings.TYPE, config -> new DummyRealm(config));
        factories.put(NativeRealmSettings.TYPE, config -> new DummyRealm(config));
        factories.put(KerberosRealmSettings.TYPE, config -> new DummyRealm(config));
        randomRealmTypesCount = randomIntBetween(2, 5);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            String name = "type_" + i;
            factories.put(name, config -> new DummyRealm(config));
        }
        licenseState = mock(MockLicenseState.class);
        licenseStateListeners = new ArrayList<>();
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.getOperationMode()).thenReturn(randomFrom(License.OperationMode.values()));
        doAnswer(inv -> {
            assertThat(inv.getArguments(), arrayWithSize(1));
            Object arg0 = inv.getArguments()[0];
            assertThat(arg0, instanceOf(LicenseStateListener.class));
            this.licenseStateListeners.add((LicenseStateListener) arg0);
            return null;
        }).when(licenseState).addListener(Mockito.any(LicenseStateListener.class));

        threadContext = new ThreadContext(Settings.EMPTY);
        reservedRealm = mock(ReservedRealm.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        allowAllRealms();
        when(reservedRealm.type()).thenReturn(ReservedRealm.TYPE);
        when(reservedRealm.name()).thenReturn("reserved");
    }

    private void allowAllRealms() {
        setRealmAvailability(type -> true);
    }

    private void allowOnlyStandardRealms() {
        setRealmAvailability(f -> f.getMinimumOperationMode() != License.OperationMode.PLATINUM);
    }

    private void allowOnlyNativeRealms() {
        setRealmAvailability(type -> false);
    }

    private void setRealmAvailability(Function<LicensedFeature.Persistent, Boolean> body) {
        InternalRealms.getConfigurableRealmsTypes().forEach(type -> {
            final LicensedFeature.Persistent feature = InternalRealms.getLicensedFeature(type);
            if (feature != null) {
                when(licenseState.isAllowed(feature)).thenReturn(body.apply(feature));
            }
        });
        when(licenseState.isAllowed(Security.CUSTOM_REALMS_FEATURE)).thenReturn(body.apply(Security.CUSTOM_REALMS_FEATURE));
        licenseStateListeners.forEach(LicenseStateListener::licenseStateChanged);
    }

    public void testRealmTypeAvailable() {
        final Set<String> basicRealmTypes = Sets.newHashSet("file", "native", "reserved");
        final Set<String> goldRealmTypes = Sets.newHashSet("ldap", "active_directory", "pki");

        final Set<String> platinumRealmTypes = new HashSet<>(InternalRealms.getConfigurableRealmsTypes());
        platinumRealmTypes.addAll(this.factories.keySet());
        platinumRealmTypes.removeAll(basicRealmTypes);
        platinumRealmTypes.removeAll(goldRealmTypes);

        Consumer<String> checkAllowed = type -> assertThat("Type: " + type, Realms.isRealmTypeAvailable(licenseState, type), is(true));
        Consumer<String> checkNotAllowed = type -> assertThat("Type: " + type, Realms.isRealmTypeAvailable(licenseState, type), is(false));

        allowAllRealms();
        platinumRealmTypes.forEach(checkAllowed);
        goldRealmTypes.forEach(checkAllowed);
        basicRealmTypes.forEach(checkAllowed);

        allowOnlyStandardRealms();
        platinumRealmTypes.forEach(checkNotAllowed);
        goldRealmTypes.forEach(checkAllowed);
        basicRealmTypes.forEach(checkAllowed);

        allowOnlyNativeRealms();
        platinumRealmTypes.forEach(checkNotAllowed);
        goldRealmTypes.forEach(checkNotAllowed);
        basicRealmTypes.forEach(checkAllowed);
    }

    public void testWithSettings() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < randomRealmTypesCount; i++) {
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        verify(licenseState, times(1)).copyCurrentLicenseState();
        verify(licenseState, times(1)).isSecurityEnabled();
        verify(licenseState, times(1)).getOperationMode();

        // Verify that we recorded licensed-feature use for each realm (this is trigger on license load during node startup)
        verify(licenseState, Mockito.atLeast(randomRealmTypesCount)).isAllowed(Security.CUSTOM_REALMS_FEATURE);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            verify(licenseState, atLeastOnce()).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "realm_" + i);
        }
        verifyNoMoreInteractions(licenseState);

        Iterator<Realm> iterator = realms.iterator();
        assertThat(iterator.hasNext(), is(true));
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));

        int i = 0;
        while (iterator.hasNext()) {
            realm = iterator.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
    }

    public void testWithSettingsWhereDifferentRealmsHaveSameOrder() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        List<Integer> randomSeq = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            randomSeq.add(i);
        }
        Collections.shuffle(randomSeq, random());

        TreeMap<String, Integer> nameToRealmId = new TreeMap<>();
        for (int i = 0; i < randomRealmTypesCount; i++) {
            int randomizedRealmId = randomSeq.get(i);
            String randomizedRealmName = randomAlphaOfLengthBetween(12, 32);
            nameToRealmId.put("realm_" + randomizedRealmName, randomizedRealmId);
            // set same order for all realms
            builder.put("xpack.security.authc.realms.type_" + randomizedRealmId + ".realm_" + randomizedRealmName + ".order", 1);
        }
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        Iterator<Realm> iterator = realms.iterator();
        assertThat(iterator.hasNext(), is(true));
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));

        // As order is same for all realms, it should fall back secondary comparison on name
        // Verify that realms are iterated in order based on name
        Iterator<String> expectedSortedOrderNames = nameToRealmId.keySet().iterator();
        while (iterator.hasNext()) {
            realm = iterator.next();
            String expectedRealmName = expectedSortedOrderNames.next();
            assertThat(realm.order(), equalTo(1));
            assertThat(realm.type(), equalTo("type_" + nameToRealmId.get(expectedRealmName)));
            assertThat(realm.name(), equalTo(expectedRealmName));
        }

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));

        assertWarnings(
            "Found multiple realms configured with the same order: [1: "
                + nameToRealmId.entrySet()
                    .stream()
                    .map(e -> "xpack.security.authc.realms.type_" + e.getValue() + "." + e.getKey() + ".order")
                    .sorted()
                    .collect(Collectors.joining(","))
                + "]. "
                + "In next major release, node will fail to start with duplicated realm order."
        );
    }

    public void testWithSettingsWithMultipleInternalRealmsOfSameType() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.file.realm_1.order", 0)
            .put("xpack.security.authc.realms.file.realm_2.order", 1)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        try {
            new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("multiple [file] realms are configured"));
        }
    }

    public void testWithSettingsWithMultipleRealmsWithSameName() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.file.realm_1.order", 0)
            .put("xpack.security.authc.realms.native.realm_1.order", 1)
            .put("xpack.security.authc.realms.kerberos.realm_1.order", 2)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        });
        assertThat(e.getMessage(), containsString("Found multiple realms configured with the same name"));
    }

    public void testWithEmptySettings() throws Exception {
        Realms realms = new Realms(
            Settings.EMPTY,
            TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build()),
            factories,
            licenseState,
            threadContext,
            reservedRealm
        );
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
    }

    public void testFeatureTrackingWithMultipleRealms() throws Exception {
        factories.put(LdapRealmSettings.LDAP_TYPE, DummyRealm::new);
        factories.put(PkiRealmSettings.TYPE, DummyRealm::new);

        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.file.file_realm.order", 0)
            .put("xpack.security.authc.realms.native.native_realm.order", 1)
            .put("xpack.security.authc.realms.kerberos.kerberos_realm.order", 2)
            .put("xpack.security.authc.realms.ldap.ldap_realm_1.order", 3)
            .put("xpack.security.authc.realms.ldap.ldap_realm_2.order", 4)
            .put("xpack.security.authc.realms.pki.pki_realm.order", 5)
            .put("xpack.security.authc.realms.type_0.custom_realm_1.order", 6)
            .put("xpack.security.authc.realms.type_1.custom_realm_2.order", 7)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);

        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getActiveRealms(), hasSize(9)); // 0..7 configured + reserved

        verify(licenseState).enableUsageTracking(Security.KERBEROS_REALM_FEATURE, "kerberos_realm");
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "ldap_realm_1");
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "ldap_realm_2");
        verify(licenseState).enableUsageTracking(Security.PKI_REALM_FEATURE, "pki_realm");
        verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_1");
        verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_2");
    }

    public void testRealmsAreDisabledOnLicenseDowngrade() throws Exception {
        factories.put(LdapRealmSettings.LDAP_TYPE, DummyRealm::new);
        factories.put(PkiRealmSettings.TYPE, DummyRealm::new);

        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.file.file_realm.order", 0)
            .put("xpack.security.authc.realms.native.native_realm.order", 1)
            .put("xpack.security.authc.realms.kerberos.kerberos_realm.order", 2)
            .put("xpack.security.authc.realms.ldap.ldap_realm_1.order", 3)
            .put("xpack.security.authc.realms.ldap.ldap_realm_2.order", 4)
            .put("xpack.security.authc.realms.pki.pki_realm.order", 5)
            .put("xpack.security.authc.realms.type_0.custom_realm_1.order", 6)
            .put("xpack.security.authc.realms.type_1.custom_realm_2.order", 7)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);

        allowAllRealms();

        final Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getActiveRealms(), hasSize(9)); // 0..7 configured + reserved

        verify(licenseState).enableUsageTracking(Security.KERBEROS_REALM_FEATURE, "kerberos_realm");
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "ldap_realm_1");
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "ldap_realm_2");
        verify(licenseState).enableUsageTracking(Security.PKI_REALM_FEATURE, "pki_realm");
        verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_1");
        verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_2");

        final Logger realmsLogger = LogManager.getLogger(Realms.class);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(realmsLogger, appender);
        appender.start();

        when(licenseState.statusDescription()).thenReturn("mock license");
        try {
            for (String realmId : Arrays.asList("kerberos.kerberos_realm", "type_0.custom_realm_1", "type_1.custom_realm_2")) {
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "Realm [" + realmId + "] disabled",
                        realmsLogger.getName(),
                        Level.WARN,
                        "The [" + realmId + "] realm has been automatically disabled due to a change in license [mock license]"
                    )
                );
            }
            allowOnlyStandardRealms();
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.removeAppender(realmsLogger, appender);
        }

        final List<String> unlicensedRealmNames = realms.getUnlicensedRealms().stream().map(r -> r.name()).collect(Collectors.toList());
        assertThat(unlicensedRealmNames, containsInAnyOrder("kerberos_realm", "custom_realm_1", "custom_realm_2"));
        assertThat(realms.getActiveRealms(), hasSize(6)); // 9 - 3

        verify(licenseState).disableUsageTracking(Security.KERBEROS_REALM_FEATURE, "kerberos_realm");
        verify(licenseState).disableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_1");
        verify(licenseState).disableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "custom_realm_2");
    }

    public void testUnlicensedWithOnlyCustomRealms() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < randomRealmTypesCount; i++) {
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        // this is the iterator when licensed
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        int i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
        for (i = 0; i < randomRealmTypesCount; i++) {
            verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "realm_" + i);
        }

        allowOnlyNativeRealms();

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(randomRealmTypesCount));
        iter = realms.getUnlicensedRealms().iterator();
        i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }

        allowOnlyNativeRealms();

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(randomRealmTypesCount));
        iter = realms.getUnlicensedRealms().iterator();
        i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }
    }

    public void testUnlicensedWithInternalRealms() throws Exception {
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(config));
        assertThat(factories.get("type_0"), notNullValue());
        String ldapRealmName = randomAlphaOfLengthBetween(3, 8);
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.ldap." + ldapRealmName + ".order", "0")
            .put("xpack.security.authc.realms.type_0.custom.order", "1");
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));

        int i = 0;
        // this is the iterator when licensed
        List<String> types = new ArrayList<>();
        while (iter.hasNext()) {
            realm = iter.next();
            i++;
            types.add(realm.type());
        }
        assertThat(types, contains("ldap", "type_0"));
        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, ldapRealmName);

        allowOnlyStandardRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.type(), is("ldap"));
            i++;
        }
        assertThat(i, is(1));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("type_0"));
        assertThat(realm.name(), equalTo("custom"));

        allowOnlyNativeRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(2));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("ldap"));
        assertThat(realm.name(), equalTo(ldapRealmName));
        realm = realms.getUnlicensedRealms().get(1);
        assertThat(realm.type(), equalTo("type_0"));
        assertThat(realm.name(), equalTo("custom"));
    }

    public void testUnlicensedWithNativeRealmSettings() throws Exception {
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(config));
        final String type = randomFrom(FileRealmSettings.TYPE, NativeRealmSettings.TYPE);
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.ldap.foo.order", "0")
            .put("xpack.security.authc.realms." + type + ".native.order", "1");
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is("ldap"));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(type));
        assertThat(iter.hasNext(), is(false));
        assertThat(realms.getUnlicensedRealms(), empty());

        // during init only
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        // each time the license state changes
        verify(licenseState, times(1)).copyCurrentLicenseState();
        verify(licenseState, times(1)).isSecurityEnabled();
        verify(licenseState, times(1)).getOperationMode();

        // Verify that we recorded licensed-feature use for each licensed realm (this is trigger on license load/change)
        verify(licenseState, times(1)).isAllowed(Security.LDAP_REALM_FEATURE);
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "foo");
        verifyNoMoreInteractions(licenseState);

        allowOnlyNativeRealms();
        // because the license state changed ...
        verify(licenseState, times(2)).copyCurrentLicenseState();
        verify(licenseState, times(2)).isSecurityEnabled();
        verify(licenseState, times(2)).getOperationMode();

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(type));
        assertThat(iter.hasNext(), is(false));

        // Verify that we checked (a 2nd time) the license for the non-basic realm
        verify(licenseState, times(2)).isAllowed(Security.LDAP_REALM_FEATURE);
        // Verify that we stopped tracking use for realms which are no longer licensed
        verify(licenseState).disableUsageTracking(Security.LDAP_REALM_FEATURE, "foo");
        verify(licenseState).statusDescription();
        verifyNoMoreInteractions(licenseState);

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("ldap"));
        assertThat(realm.name(), equalTo("foo"));
    }

    public void testUnlicensedWithNonStandardRealms() throws Exception {
        final String selectedRealmType = randomFrom(SamlRealmSettings.TYPE, KerberosRealmSettings.TYPE, OpenIdConnectRealmSettings.TYPE);
        factories.put(selectedRealmType, config -> new DummyRealm(config));
        final LicensedFeature.Persistent feature = InternalRealms.getLicensedFeature(selectedRealmType);
        String realmName = randomAlphaOfLengthBetween(3, 8);
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms." + selectedRealmType + "." + realmName + ".order", "0");
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(selectedRealmType));
        assertThat(iter.hasNext(), is(false));
        assertThat(realms.getUnlicensedRealms(), empty());
        verify(licenseState, times(1)).isAllowed(feature);
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);

        allowOnlyStandardRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo(selectedRealmType));
        assertThat(realm.name(), equalTo(realmName));

        verify(licenseState, times(2)).isAllowed(feature);
        verify(licenseState, times(1)).disableUsageTracking(feature, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);

        allowOnlyNativeRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(FileRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(NativeRealmSettings.TYPE));
        assertThat(iter.hasNext(), is(false));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo(selectedRealmType));
        assertThat(realm.name(), equalTo(realmName));

        verify(licenseState, times(3)).isAllowed(feature);
        // this doesn't get called a second time because it didn't change
        verify(licenseState, times(1)).disableUsageTracking(feature, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);
    }

    public void testDisabledRealmsAreNotAdded() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        disableFileAndNativeRealms(builder);
        List<Integer> orders = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        boolean anyEnabled = false;
        for (int i = 0; i < randomRealmTypesCount; i++) {
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", orders.get(i));
            boolean enabled = randomBoolean();
            anyEnabled = anyEnabled || enabled;
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".enabled", enabled);
            if (enabled) {
                orderToIndex.put(orders.get(i), i);
                logger.error("put [{}] -> [{}]", orders.get(i), i);
            }
        }
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        if (false == anyEnabled) {
            assertWarnings(
                "Found explicitly disabled basic realms: [file,native]. "
                    + "But they will be enabled because no other realms are configured or enabled. "
                    + "In next major release, explicitly disabled basic realms will remain disabled."
            );
        }
        Iterator<Realm> iterator = realms.iterator();
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iterator.hasNext(), is(true));

        int count = 0;
        while (iterator.hasNext()) {
            realm = iterator.next();
            Integer index = orderToIndex.get(realm.order());
            if (index == null) {
                // Default realms are inserted when factories size is 1 and enabled is false
                assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
                assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
                assertThat(iterator.hasNext(), is(true));
                realm = iterator.next();
                assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
                assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
                assertThat(iterator.hasNext(), is(false));
            } else {
                assertThat(realm.type(), equalTo("type_" + index));
                assertThat(realm.name(), equalTo("realm_" + index));
                assertThat(settings.getAsBoolean("xpack.security.authc.realms.realm_" + index + ".enabled", true), equalTo(Boolean.TRUE));
                count++;
            }
        }

        assertThat(count, equalTo(orderToIndex.size()));
        assertThat(realms.getUnlicensedRealms(), empty());

        // check that disabled realms are not included in unlicensed realms
        allowOnlyNativeRealms();
        assertThat(realms.getUnlicensedRealms(), hasSize(orderToIndex.size()));
    }

    public void testAuthcAuthzDisabled() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".realm_1.order", 0);
        disableFileAndNativeRealms(builder);
        final Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        assertThat(realms.iterator().hasNext(), is(true));

        when(licenseState.isSecurityEnabled()).thenReturn(false);
        licenseStateListeners.forEach(LicenseStateListener::licenseStateChanged);
        assertThat(realms.iterator().hasNext(), is(false));
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        // test realms with duplicate values
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.type_0.foo.order", "0")
            .put("xpack.security.authc.realms.type_0.bar.order", "1");
        disableFileAndNativeRealms(builder);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realms.usageStats(future);
        Map<String, Object> usageStats = future.get();
        assertThat(usageStats.size(), is(factories.size()));

        // first check type_0
        assertThat(usageStats.get("type_0"), instanceOf(Map.class));
        Map<String, Object> type0Map = (Map<String, Object>) usageStats.get("type_0");
        assertThat(type0Map, hasEntry("enabled", true));
        assertThat(type0Map, hasEntry("available", true));
        assertThat((Iterable<? extends String>) type0Map.get("name"), contains("foo", "bar"));
        assertThat((Iterable<? extends Integer>) type0Map.get("order"), contains(0, 1));

        for (Entry<String, Object> entry : usageStats.entrySet()) {
            String type = entry.getKey();
            if ("type_0".equals(type)) {
                continue;
            }

            Map<String, Object> typeMap = (Map<String, Object>) entry.getValue();
            assertThat(typeMap, hasEntry("enabled", false));
            assertThat(typeMap, hasEntry("available", true));
            assertThat(typeMap.size(), is(2));
        }

        // check standard realms include native
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        allowOnlyStandardRealms();
        future = new PlainActionFuture<>();
        realms.usageStats(future);
        usageStats = future.get();
        assertThat(usageStats.size(), is(factories.size()));
        for (Entry<String, Object> entry : usageStats.entrySet()) {
            final String type = entry.getKey();
            Map<String, Object> typeMap = (Map<String, Object>) entry.getValue();
            if (FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type)) {
                assertThat(typeMap, hasEntry("enabled", true));
                assertThat(typeMap, hasEntry("available", true));
                assertThat((Iterable<? extends String>) typeMap.get("name"), contains("default_" + type));
            } else {
                assertThat(typeMap, hasEntry("enabled", false));
                assertThat(typeMap, hasEntry("available", false));
                assertThat(typeMap.size(), is(2));
            }
        }
    }

    public void testInitRealmsFailsForMultipleKerberosRealms() throws IOException {
        final Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        builder.put("xpack.security.authc.realms.kerberos.realm_1.order", 1);
        builder.put("xpack.security.authc.realms.kerberos.realm_2.order", 2);
        final Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        final IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> new Realms(settings, env, factories, licenseState, threadContext, reservedRealm)
        );
        assertThat(
            iae.getMessage(),
            is(
                equalTo(
                    "multiple realms [realm_1, realm_2] configured of type [kerberos], [kerberos] can only have one such realm configured"
                )
            )
        );
    }

    public void testWarningForMissingRealmOrder() throws Exception {
        final int realmTypeId = randomIntBetween(0, randomRealmTypesCount - 1);
        final String realmName = randomAlphaOfLengthBetween(4, 12);
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.type_" + realmTypeId + ".realm_" + realmName + ".enabled", true);
        disableFileAndNativeRealms(builder);
        final Settings settings = builder.build();

        new Realms(settings, TestEnvironment.newEnvironment(settings), factories, licenseState, threadContext, reservedRealm);
        assertWarnings(
            "Found realms without order config: [xpack.security.authc.realms.type_"
                + realmTypeId
                + ".realm_"
                + realmName
                + ".order]. "
                + "In next major release, node will fail to start with missing realm order."
        );
    }

    public void testWarningsForImplicitlyDisabledBasicRealms() throws Exception {
        final Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        final boolean otherRealmConfigured = randomBoolean();
        final boolean otherRealmEnabled = randomBoolean();
        if (otherRealmConfigured) {
            final int otherRealmId = randomIntBetween(0, randomRealmTypesCount - 1);
            final String otherRealmName = randomAlphaOfLengthBetween(4, 12);
            if (otherRealmEnabled) {
                builder.put("xpack.security.authc.realms.type_" + otherRealmId + ".realm_" + otherRealmName + ".order", 1);
            } else {
                builder.put("xpack.security.authc.realms.type_" + otherRealmId + ".realm_" + otherRealmName + ".enabled", false);
            }
        }
        final boolean fileRealmConfigured = randomBoolean();
        final boolean fileRealmEnabled = randomBoolean();
        if (fileRealmConfigured) {
            final String fileRealmName = randomAlphaOfLengthBetween(4, 12);
            // Configure file realm or explicitly disable it
            if (fileRealmEnabled) {
                builder.put("xpack.security.authc.realms.file." + fileRealmName + ".order", 10);
            } else {
                builder.put("xpack.security.authc.realms.file." + fileRealmName + ".enabled", false);
            }
        }
        final boolean nativeRealmConfigured = randomBoolean();
        final boolean nativeRealmEnabled = randomBoolean();
        if (nativeRealmConfigured) {
            final String nativeRealmName = randomAlphaOfLengthBetween(4, 12);
            // Configure native realm or explicitly disable it
            if (nativeRealmEnabled) {
                builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".order", 20);
            } else {
                builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".enabled", false);
            }
        }
        final Settings settings = builder.build();
        final Realms realms = new Realms(
            settings,
            TestEnvironment.newEnvironment(settings),
            factories,
            licenseState,
            threadContext,
            reservedRealm
        );

        if (otherRealmConfigured && otherRealmEnabled) {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                assertWarnings(
                    "Found implicitly disabled basic realms: [file,native]. "
                        + "They are disabled because there are other explicitly configured realms. "
                        + "In next major release, basic realms will always be enabled unless explicitly disabled."
                );
            } else if (false == fileRealmConfigured) {
                assertWarnings(
                    "Found implicitly disabled basic realm: [file]. "
                        + "It is disabled because there are other explicitly configured realms. "
                        + "In next major release, basic realms will always be enabled unless explicitly disabled."
                );
            } else if (false == nativeRealmConfigured) {
                assertWarnings(
                    "Found implicitly disabled basic realm: [native]. "
                        + "It is disabled because there are other explicitly configured realms. "
                        + "In next major release, basic realms will always be enabled unless explicitly disabled."
                );
            }
        } else {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                // Default behaviour of implicitly enabling file and native realms
                assertNotNull(realms.realm(FileRealmSettings.DEFAULT_NAME));
                assertNotNull(realms.realm(NativeRealmSettings.DEFAULT_NAME));
            } else if (false == fileRealmConfigured) {
                if (nativeRealmEnabled) {
                    assertWarnings(
                        "Found implicitly disabled basic realm: [file]. "
                            + "It is disabled because there are other explicitly configured realms. "
                            + "In next major release, basic realms will always be enabled unless explicitly disabled."
                    );
                } else {
                    assertWarnings(
                        "Found explicitly disabled basic realm: [native]. "
                            + "But it will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled."
                    );
                    assertNotNull(realms.realm(FileRealmSettings.DEFAULT_NAME));
                    assertNotNull(realms.realm(NativeRealmSettings.DEFAULT_NAME));
                }
            } else if (false == nativeRealmConfigured) {
                if (fileRealmEnabled) {
                    assertWarnings(
                        "Found implicitly disabled basic realm: [native]. "
                            + "It is disabled because there are other explicitly configured realms. "
                            + "In next major release, basic realms will always be enabled unless explicitly disabled."
                    );
                } else {
                    assertWarnings(
                        "Found explicitly disabled basic realm: [file]. "
                            + "But it will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled."
                    );
                    assertNotNull(realms.realm(FileRealmSettings.DEFAULT_NAME));
                    assertNotNull(realms.realm(NativeRealmSettings.DEFAULT_NAME));
                }
            } else {
                if (false == fileRealmEnabled && false == nativeRealmEnabled) {
                    assertWarnings(
                        "Found explicitly disabled basic realms: [file,native]. "
                            + "But they will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled."
                    );
                    assertNotNull(realms.realm(FileRealmSettings.DEFAULT_NAME));
                    assertNotNull(realms.realm(NativeRealmSettings.DEFAULT_NAME));
                }
            }
        }
    }

    public void testWarningsForReservedPrefixedRealmNames() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        final boolean invalidFileRealmName = randomBoolean();
        final boolean invalidNativeRealmName = randomBoolean();
        // Ensure at least one realm has invalid name
        final int upperBound = (invalidFileRealmName || invalidNativeRealmName) ? randomRealmTypesCount : randomRealmTypesCount - 1;
        final int invalidOtherRealmNameIndex = randomIntBetween(0, upperBound);

        final List<String> invalidRealmNames = new ArrayList<>();
        if (invalidFileRealmName) {
            builder.put("xpack.security.authc.realms.file._default_file.order", -20);
            invalidRealmNames.add("xpack.security.authc.realms.file._default_file");
        } else {
            builder.put("xpack.security.authc.realms.file.default_file.order", -20);
        }

        if (invalidNativeRealmName) {
            builder.put("xpack.security.authc.realms.native._default_native.order", -10);
            invalidRealmNames.add("xpack.security.authc.realms.native._default_native");
        } else {
            builder.put("xpack.security.authc.realms.native.default_native.order", -10);
        }

        IntStream.range(0, randomRealmTypesCount).forEach(i -> {
            if (i != invalidOtherRealmNameIndex) {
                builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", i)
                    .put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".enabled", randomBoolean());
            } else {
                builder.put("xpack.security.authc.realms.type_" + i + "._realm_" + i + ".order", i)
                    .put("xpack.security.authc.realms.type_" + i + "._realm_" + i + ".enabled", randomBoolean());
                invalidRealmNames.add("xpack.security.authc.realms.type_" + i + "._realm_" + i);
            }
        });

        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        assertWarnings(
            Level.WARN,
            "Found realm "
                + (invalidRealmNames.size() == 1 ? "name" : "names")
                + " with reserved prefix [_]: ["
                + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ")
                + "]. "
                + "In a future major release, node will fail to start if any realm names start with reserved prefix."
        );
    }

    private void disableFileAndNativeRealms(Settings.Builder builder) {
        builder.put("xpack.security.authc.realms.file.default_file.enabled", false)
            .put("xpack.security.authc.realms.native.default_native.enabled", false);
    }

    static class DummyRealm extends Realm {

        DummyRealm(RealmConfig config) {
            super(config);
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return false;
        }

        @Override
        public AuthenticationToken token(ThreadContext threadContext) {
            return null;
        }

        @Override
        public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult> listener) {
            listener.onResponse(AuthenticationResult.notHandled());
        }

        @Override
        public void lookupUser(String username, ActionListener<User> listener) {
            listener.onResponse(null);
        }
    }
}
