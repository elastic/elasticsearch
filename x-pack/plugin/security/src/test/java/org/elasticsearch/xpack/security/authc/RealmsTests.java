/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
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
        factories.put(FileRealmSettings.TYPE, config -> new DummyRealm(FileRealmSettings.TYPE, config));
        factories.put(NativeRealmSettings.TYPE, config -> new DummyRealm(NativeRealmSettings.TYPE, config));
        factories.put(KerberosRealmSettings.TYPE, config -> new DummyRealm(KerberosRealmSettings.TYPE, config));
        randomRealmTypesCount = randomIntBetween(2, 5);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            String name = "type_" + i;
            factories.put(name, config -> new DummyRealm(name, config));
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
        allowAllRealms();
        when(reservedRealm.type()).thenReturn(ReservedRealm.TYPE);
        when(reservedRealm.name()).thenReturn("reserved");
    }

    private void allowAllRealms() {
        when(licenseState.isAllowed(Security.ALL_REALMS_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(Security.STANDARD_REALMS_FEATURE)).thenReturn(true);
        licenseStateListeners.forEach(LicenseStateListener::licenseStateChanged);
    }

    private void allowOnlyStandardRealms() {
        when(licenseState.isAllowed(Security.ALL_REALMS_FEATURE)).thenReturn(false);
        when(licenseState.isAllowed(Security.STANDARD_REALMS_FEATURE)).thenReturn(true);
        licenseStateListeners.forEach(LicenseStateListener::licenseStateChanged);
    }

    private void allowOnlyNativeRealms() {
        when(licenseState.isAllowed(Security.ALL_REALMS_FEATURE)).thenReturn(false);
        when(licenseState.isAllowed(Security.STANDARD_REALMS_FEATURE)).thenReturn(false);
        licenseStateListeners.forEach(LicenseStateListener::licenseStateChanged);
    }

    public void testWithSettings() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
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
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);

        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        verify(licenseState, times(1)).copyCurrentLicenseState();
        verify(licenseState, times(1)).getOperationMode();

        // Verify that we recorded licensed-feature use for each realm (this is trigger on license load during node startup)
        verify(licenseState, Mockito.atLeast(randomRealmTypesCount)).isAllowed(Security.ALL_REALMS_FEATURE);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            verify(licenseState, atLeastOnce()).enableUsageTracking(Security.ALL_REALMS_FEATURE, "realm_" + i);
        }
        verifyNoMoreInteractions(licenseState);

        Iterator<Realm> iterator = realms.iterator();
        assertThat(iterator.hasNext(), is(true));
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iterator, fileRealmDisabled, nativeRealmDisabled);

        int i = 0;
        while (iterator.hasNext()) {
            realm = iterator.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
            if (i == randomRealmTypesCount) {
                break;
            }
        }

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
    }

    public void testWithSettingsWhereDifferentRealmsHaveSameOrder() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
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
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->{
            new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        });
        assertThat(e.getMessage(), containsString("Found multiple realms configured with the same order"));
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->{
            new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        });
        assertThat(e.getMessage(), containsString("Found multiple realms configured with the same name"));
    }

    public void testWithEmptySettings() throws Exception {
        Realms realms = new Realms(Settings.EMPTY, TestEnvironment.newEnvironment(Settings.builder().put("path.home",
                createTempDir()).build()), factories, licenseState, threadContext, reservedRealm);
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

    public void testUnlicensedWithOnlyCustomRealms() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
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
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        // this is the iterator when licensed
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);

        int i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
            if (i == randomRealmTypesCount) {
                break;
            }
        }

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
        for (i = 0; i < randomRealmTypesCount; i++) {
            verify(licenseState).enableUsageTracking(Security.ALL_REALMS_FEATURE, "realm_" + i);
        }

        allowOnlyNativeRealms();

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);

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
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(LdapRealmSettings.LDAP_TYPE, config));
        assertThat(factories.get("type_0"), notNullValue());
        String ldapRealmName = randomAlphaOfLengthBetween(3, 8);
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.security.authc.realms.ldap." + ldapRealmName + ".order", "0")
                .put("xpack.security.authc.realms.type_0.custom.order", "1");
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);
        assertTrue(iter.hasNext());
        realm = iter.next();
        assertThat(realm.type(), is("ldap"));
        assertTrue(iter.hasNext());
        realm = iter.next();
        assertThat(realm.type(), is("type_0"));

        assertThat(realms.getUnlicensedRealms(), empty());
        assertThat(realms.getUnlicensedRealms(), sameInstance(realms.getUnlicensedRealms()));
        verify(licenseState).enableUsageTracking(Security.STANDARD_REALMS_FEATURE, ldapRealmName);

        allowOnlyStandardRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);
        assertTrue(iter.hasNext());
        realm = iter.next();
        assertThat(realm.type(), is("ldap"));

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("type_0"));
        assertThat(realm.name(), equalTo("custom"));

        allowOnlyNativeRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(2));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("ldap"));
        assertThat(realm.name(), equalTo(ldapRealmName));
        realm = realms.getUnlicensedRealms().get(1);
        assertThat(realm.type(), equalTo("type_0"));
        assertThat(realm.name(), equalTo("custom"));
    }

    public void testUnlicensedWithBasicRealmSettings() throws Exception {
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(LdapRealmSettings.LDAP_TYPE, config));
        final String type = randomFrom(FileRealmSettings.TYPE, NativeRealmSettings.TYPE);
        final String otherType = FileRealmSettings.TYPE.equals(type) ? NativeRealmSettings.TYPE : FileRealmSettings.TYPE;
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.security.authc.realms.ldap.foo.order", "0")
                .put("xpack.security.authc.realms." + type + ".native.order", "1");
        final boolean otherTypeDisabled = randomDisableRealm(builder, otherType);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        if (false == otherTypeDisabled) {
            assertThat(iter.hasNext(), is(true));
            realm = iter.next();
            assertThat(realm.type(), is(otherType));
        }
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is("ldap"));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(type));
        assertThat(iter.hasNext(), is(false));
        assertThat(realms.getUnlicensedRealms(), empty());

        // during init
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        // each time the license changes ...
        verify(licenseState, times(1)).copyCurrentLicenseState();
        verify(licenseState, times(1)).getOperationMode();

        // Verify that we recorded licensed-feature use for each licensed realm (this is trigger on license load/change)
        verify(licenseState, times(1)).isAllowed(Security.STANDARD_REALMS_FEATURE);
        verify(licenseState).enableUsageTracking(Security.STANDARD_REALMS_FEATURE, "foo");
        verifyNoMoreInteractions(licenseState);

        allowOnlyNativeRealms();
        // because the license state changed ...
        verify(licenseState, times(2)).copyCurrentLicenseState();
        verify(licenseState, times(2)).getOperationMode();

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        if (false == otherTypeDisabled) {
            assertThat(iter.hasNext(), is(true));
            realm = iter.next();
            assertThat(realm.type(), is(otherType));
        }
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(type));
        assertThat(iter.hasNext(), is(false));

        // Verify that we checked (a 2nd time) the license for the non-basic realm
        verify(licenseState, times(2)).isAllowed(Security.STANDARD_REALMS_FEATURE);
        // Verify that we stopped tracking  use for realms which are no longer licensed
        verify(licenseState).disableUsageTracking(Security.STANDARD_REALMS_FEATURE, "foo");
        verifyNoMoreInteractions(licenseState);

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo("ldap"));
        assertThat(realm.name(), equalTo("foo"));
    }

    public void testUnlicensedWithNonStandardRealms() throws Exception {
        final String selectedRealmType = randomFrom(SamlRealmSettings.TYPE, KerberosRealmSettings.TYPE, OpenIdConnectRealmSettings.TYPE);
        factories.put(selectedRealmType, config -> new DummyRealm(selectedRealmType, config));
        String realmName = randomAlphaOfLengthBetween(3, 8);
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms." + selectedRealmType + "." + realmName + ".order", "0");
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(selectedRealmType));
        assertThat(realms.getUnlicensedRealms(), empty());
        verify(licenseState, times(1)).isAllowed(Security.ALL_REALMS_FEATURE);
        verify(licenseState, times(1)).enableUsageTracking(Security.ALL_REALMS_FEATURE, realmName);

        allowOnlyStandardRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo(selectedRealmType));
        assertThat(realm.name(), equalTo(realmName));

        verify(licenseState, times(2)).isAllowed(Security.ALL_REALMS_FEATURE);
        verify(licenseState, times(1)).disableUsageTracking(Security.ALL_REALMS_FEATURE, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(Security.ALL_REALMS_FEATURE, realmName);

        allowOnlyNativeRealms();
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iter, fileRealmDisabled, nativeRealmDisabled);

        assertThat(realms.getUnlicensedRealms(), iterableWithSize(1));
        realm = realms.getUnlicensedRealms().get(0);
        assertThat(realm.type(), equalTo(selectedRealmType));
        assertThat(realm.name(), equalTo(realmName));

        verify(licenseState, times(3)).isAllowed(Security.ALL_REALMS_FEATURE);
        // this doesn't get called a second time because it didn't change
        verify(licenseState, times(1)).disableUsageTracking(Security.ALL_REALMS_FEATURE, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(Security.ALL_REALMS_FEATURE, realmName);
    }

    public void testDisabledRealmsAreNotAdded() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < randomRealmTypesCount; i++) {
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", orders.get(i));
            boolean enabled = randomBoolean();
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".enabled", enabled);
            if (enabled) {
                orderToIndex.put(orders.get(i), i);
                logger.error("put [{}] -> [{}]", orders.get(i), i);
            }
        }
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iterator = realms.iterator();
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));
        assertImplicitlyAddedBasicRealms(iterator, fileRealmDisabled, nativeRealmDisabled);

        int count = 0;
        while (iterator.hasNext()) {
            realm = iterator.next();
            Integer index = orderToIndex.get(realm.order());
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            assertThat(settings.getAsBoolean("xpack.security.authc.realms.realm_" + index + ".enabled", true), equalTo(Boolean.TRUE));
            count++;
        }

        assertThat(count, equalTo(orderToIndex.size()));
        assertThat(realms.getUnlicensedRealms(), empty());

        // check that disabled realms are not included in unlicensed realms
        allowOnlyNativeRealms();
        assertThat(realms.getUnlicensedRealms(), hasSize(orderToIndex.size()));
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        // test realms with duplicate values
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.security.authc.realms.type_0.foo.order", "0")
                .put("xpack.security.authc.realms.type_0.bar.order", "1");
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);
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
            final boolean enabled;
            final int size;
            if (FileRealmSettings.TYPE.equals(type)) {
                enabled = fileRealmDisabled == false;
                size = enabled ? 4 : 2;
            } else if (NativeRealmSettings.TYPE.equals(type)) {
                enabled = nativeRealmDisabled == false;
                size = enabled ? 4 : 2;
            } else {
                enabled = false;
                size = 2;
            }
            assertThat(typeMap, hasEntry("enabled", enabled));
            assertThat(typeMap, hasEntry("available", true));
            assertThat(typeMap.size(), is(size));
        }

        // check standard realms include native
        allowOnlyStandardRealms();
        future = new PlainActionFuture<>();
        realms.usageStats(future);
        usageStats = future.get();
        assertThat(usageStats.size(), is(factories.size()));
        for (Entry<String, Object> entry : usageStats.entrySet()) {
            final String type = entry.getKey();
            Map<String, Object> typeMap = (Map<String, Object>) entry.getValue();
            if (FileRealmSettings.TYPE.equals(type)) {
                assertThat(typeMap, hasEntry("available", true));
                if (false == fileRealmDisabled) {
                    assertThat(typeMap, hasEntry("enabled", true));
                    assertThat((Iterable<? extends String>) typeMap.get("name"), contains(FileRealmSettings.DEFAULT_NAME));
                }
            } else if (NativeRealmSettings.TYPE.equals(type)) {

                assertThat(typeMap, hasEntry("available", true));
                if (false == nativeRealmDisabled) {
                    assertThat(typeMap, hasEntry("enabled", true));
                    assertThat((Iterable<? extends String>) typeMap.get("name"), contains(NativeRealmSettings.DEFAULT_NAME));
                }
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
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> new Realms(settings, env, factories, licenseState, threadContext, reservedRealm));
        assertThat(iae.getMessage(), is(equalTo(
                "multiple realms [realm_1, realm_2] configured of type [kerberos], [kerberos] can only have one such realm configured")));
    }

    public void testWarningsForReservedPrefixedRealmNames() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir());
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

        assertWarnings("Found realm " + (invalidRealmNames.size() == 1 ? "name" : "names")
            + " with reserved prefix [_]: ["
            + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ") + "]. "
            + "In a future major release, node will fail to start if any realm names start with reserved prefix.");
    }

    private boolean randomDisableRealm(Settings.Builder builder, String type) {
        final boolean disabled = randomBoolean();
        if (disabled) {
            builder.put("xpack.security.authc.realms." + type + ".native.enabled", false);
        }
        return disabled;
    }

    private void assertImplicitlyAddedBasicRealms(Iterator<Realm> iter, boolean fileRealmDisabled, boolean nativeRealmDisabled) {
        Realm realm;
        if (false == fileRealmDisabled && false == nativeRealmDisabled) {
            assertTrue(iter.hasNext());
            realm = iter.next();
            assertThat(realm.type(), is(FileRealmSettings.TYPE));
            assertThat(realm.name(), is(FileRealmSettings.DEFAULT_NAME));
            assertTrue(iter.hasNext());
            realm = iter.next();
            assertThat(realm.type(), is(NativeRealmSettings.TYPE));
            assertThat(realm.name(), is(NativeRealmSettings.DEFAULT_NAME));
        } else if (false == fileRealmDisabled) {
            assertTrue(iter.hasNext());
            realm = iter.next();
            assertThat(realm.type(), is(FileRealmSettings.TYPE));
            assertThat(realm.name(), is(FileRealmSettings.DEFAULT_NAME));
        } else if (false == nativeRealmDisabled) {
            assertTrue(iter.hasNext());
            realm = iter.next();
            assertThat(realm.type(), is(NativeRealmSettings.TYPE));
            assertThat(realm.name(), is(NativeRealmSettings.DEFAULT_NAME));
        }
    }

    static class DummyRealm extends Realm {

        DummyRealm(String type, RealmConfig config) {
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
