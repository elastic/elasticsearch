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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
        randomRealmTypesCount = randomIntBetween(2, 9);
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

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        reservedRealm = new ReservedRealm(
            mock(Environment.class),
            Settings.EMPTY,
            mock(NativeUsersStore.class),
            new AnonymousUser(Settings.EMPTY),
            threadPool
        );
        allowAllRealms();
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
        final boolean fileRealmDisabled = randomDisableRealm(builder, FileRealmSettings.TYPE);
        final boolean nativeRealmDisabled = randomDisableRealm(builder, NativeRealmSettings.TYPE);

        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        verify(licenseState, times(1)).copyCurrentLicenseState();
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

    public void testDomainAssignment() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(randomRealmTypesCount);
        for (int i = 0; i < randomRealmTypesCount; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        List<String> domains = randomList(1, 3, () -> randomAlphaOfLengthBetween(1, 3));
        domains.add(null); // not all realms ought to have a domain
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        Map<Integer, String> indexToDomain = new HashMap<>();
        for (int i = 0; i < randomRealmTypesCount; i++) {
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".order", orders.get(i));
            builder.put("xpack.security.authc.realms.type_" + i + ".realm_" + i + ".enabled", randomBoolean());
            orderToIndex.put(orders.get(i), i);
            indexToDomain.put(i, randomFrom(domains));
        }
        final boolean fileRealmDisabled = randomBoolean();
        if (fileRealmDisabled) {
            builder.put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".default_file.enabled", false);
        }
        final String fileRealmDomain = randomFrom(domains);
        final boolean nativeRealmDisabled = randomBoolean();
        if (nativeRealmDisabled) {
            builder.put("xpack.security.authc.realms." + NativeRealmSettings.TYPE + ".default_native.enabled", false);
        }
        final String nativeRealmDomain = randomFrom(domains);

        Map<String, Set<RealmConfig.RealmIdentifier>> realmsForDomain = new HashMap<>();
        for (String domain : domains) {
            if (domain != null) {
                realmsForDomain.computeIfAbsent(domain, k -> new HashSet<>());
                for (Map.Entry<Integer, String> indexAndDomain : indexToDomain.entrySet()) {
                    if (domain.equals(indexAndDomain.getValue())) {
                        realmsForDomain.get(domain)
                            .add(new RealmConfig.RealmIdentifier("type_" + indexAndDomain.getKey(), "realm_" + indexAndDomain.getKey()));
                    }
                }
                if (domain.equals(fileRealmDomain)) {
                    realmsForDomain.get(domain).add(new RealmConfig.RealmIdentifier("file", "default_file"));
                }
                if (domain.equals(nativeRealmDomain)) {
                    realmsForDomain.get(domain).add(new RealmConfig.RealmIdentifier("native", "default_native"));
                }
                if (false == realmsForDomain.get(domain).isEmpty() || randomBoolean()) {
                    builder.put(
                        "xpack.security.authc.domains." + domain + ".realms",
                        realmsForDomain.get(domain).stream().map(RealmConfig.RealmIdentifier::getName).collect(Collectors.joining(", "))
                    );
                }
            }
        }

        String nodeName = randomAlphaOfLength(10);
        builder.put(Node.NODE_NAME_SETTING.getKey(), nodeName);

        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);

        Iterator<Realm> iterator = realms.iterator();
        assertThat(iterator.hasNext(), is(true));
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));
        assertThat(reservedRealm.realmRef().getDomain(), nullValue());
        if (false == fileRealmDisabled) {
            assertTrue(iterator.hasNext());
            realm = iterator.next();
            assertThat(realm.type(), is(FileRealmSettings.TYPE));
            assertThat(realm.name(), is(FileRealmSettings.DEFAULT_NAME));
            assertDomainForRealm(realm, nodeName, realmsForDomain);
        }
        if (false == nativeRealmDisabled) {
            assertTrue(iterator.hasNext());
            realm = iterator.next();
            assertThat(realm.type(), is(NativeRealmSettings.TYPE));
            assertThat(realm.name(), is(NativeRealmSettings.DEFAULT_NAME));
            assertDomainForRealm(realm, nodeName, realmsForDomain);
        }

        while (iterator.hasNext()) {
            int prevOrder = realm.order();
            realm = iterator.next();
            assertThat(realm.order(), greaterThan(prevOrder));
            int index = orderToIndex.get(realm.order());
            assertThat(realm.type(), is("type_" + index));
            assertThat(realm.name(), is("realm_" + index));
            assertDomainForRealm(realm, nodeName, realmsForDomain);
        }
    }

    private void assertDomainForRealm(Realm realm, String nodeName, Map<String, Set<RealmConfig.RealmIdentifier>> realmsByDomainName) {
        for (var domainRealmIds : realmsByDomainName.entrySet()) {
            if (domainRealmIds.getValue().contains(new RealmConfig.RealmIdentifier(realm.type(), realm.name()))) {
                RealmDomain domain = new RealmDomain(domainRealmIds.getKey(), domainRealmIds.getValue());
                assertThat(realm.realmRef(), is(new Authentication.RealmRef(realm.name(), realm.type(), nodeName, domain)));
                return;
            }
        }
        assertThat(realm.realmRef(), is(new Authentication.RealmRef(realm.name(), realm.type(), nodeName, null)));
    }

    public void testRealmAssignedToMultipleDomainsNotPermitted() {
        int realm = randomIntBetween(0, randomRealmTypesCount - 1);
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.type_" + realm + ".realm_" + realm + ".order", 2)
            .put("xpack.security.authc.realms.type_" + realm + ".other_realm_" + realm + ".order", 1)
            .put("xpack.security.authc.realms.type_" + realm + ".yet_another_realm_" + realm + ".order", 5)
            .put("xpack.security.authc.domains.domain1.realms", "realm_" + realm + ", other_realm_" + realm)
            .put("xpack.security.authc.domains.domain2.realms", "realm_" + realm + ", yet_another_realm_" + realm)
            .put("xpack.security.authc.domains.domain3.realms", "default_file, default_native")
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Realms(settings, env, factories, licenseState, threadContext, reservedRealm)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Realms can be associated to at most one domain, but realm [realm_"
                    + realm
                    + "] is associated to domains [domain1, domain2]"
            )
        );
    }

    public void testReservedRealmIsAlwaysDomainless() throws Exception {
        int realmId = randomIntBetween(0, randomRealmTypesCount - 1);
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.type_" + realmId + ".reserved.order", 2)
            .put("xpack.security.authc.domains.domain_reserved.realms", "reserved")
            .put("xpack.security.authc.realms." + NativeRealmSettings.TYPE + ".disabled_native.enabled", false)
            .put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".disabled_file.enabled", false)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        Realms realms = new Realms(settings, env, factories, licenseState, threadContext, reservedRealm);
        Iterator<Realm> iterator = realms.iterator();
        assertThat(iterator.hasNext(), is(true));
        Realm realm = iterator.next();
        assertThat(realm, is(reservedRealm));
        realm = iterator.next();
        assertThat(realm.type(), is("type_" + realmId));
        assertThat(realm.name(), is("reserved"));
        assertThat(realm.realmRef().getDomain().name(), is("domain_reserved"));
    }

    public void testDomainWithUndefinedRealms() {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.domains.test_domain.realms", "reserved, default_file")
            .put("xpack.security.authc.domains.test_domain_2.realms", "default_native")
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Realms(settings, env, factories, licenseState, threadContext, reservedRealm)
        );
        assertThat(e.getMessage(), containsString("Undefined realms [reserved] cannot be assigned to domains"));

        String syntheticRealm = randomFrom(
            ServiceAccountSettings.REALM_NAME,
            AuthenticationField.ATTACH_REALM_NAME,
            AuthenticationField.ANONYMOUS_REALM_NAME,
            AuthenticationField.API_KEY_REALM_NAME,
            AuthenticationField.FALLBACK_REALM_NAME
        );
        Settings settings2 = Settings.builder()
            .put("xpack.security.authc.domains.test_domain.realms", "default_file, " + syntheticRealm)
            .put("xpack.security.authc.domains.test_domain_2.realms", "default_native")
            .put("path.home", createTempDir())
            .build();
        Environment env2 = TestEnvironment.newEnvironment(settings2);
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new Realms(settings2, env2, factories, licenseState, threadContext, reservedRealm)
        );
        assertThat(e2.getMessage(), containsString("Undefined realms [" + syntheticRealm + "] cannot be assigned to domains"));
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
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new Realms(settings, env, factories, licenseState, threadContext, reservedRealm); }
        );
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
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> { new Realms(settings, env, factories, licenseState, threadContext, reservedRealm); }
        );
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
        assertThat(realm.realmRef().getDomain(), nullValue());
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealmSettings.TYPE));
        assertThat(realm.realmRef().getDomain(), nullValue());
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealmSettings.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealmSettings.TYPE));
        assertThat(realm.realmRef().getDomain(), nullValue());
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
            for (String realmId : List.of("kerberos.kerberos_realm", "type_0.custom_realm_1", "type_1.custom_realm_2")) {
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
            verify(licenseState).enableUsageTracking(Security.CUSTOM_REALMS_FEATURE, "realm_" + i);
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
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(config));
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
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, ldapRealmName);

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
        factories.put(LdapRealmSettings.LDAP_TYPE, config -> new DummyRealm(config));
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

        // during init only
        verify(licenseState, times(1)).addListener(Mockito.any(LicenseStateListener.class));
        // each time the license state changes
        verify(licenseState, times(1)).copyCurrentLicenseState();
        verify(licenseState, times(1)).getOperationMode();

        // Verify that we recorded licensed-feature use for each licensed realm (this is trigger on license load/change)
        verify(licenseState, times(1)).isAllowed(Security.LDAP_REALM_FEATURE);
        verify(licenseState).enableUsageTracking(Security.LDAP_REALM_FEATURE, "foo");
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
        final List<String> platinumRealms = CollectionUtils.arrayAsArrayList(
            SamlRealmSettings.TYPE,
            KerberosRealmSettings.TYPE,
            OpenIdConnectRealmSettings.TYPE,
            JwtRealmSettings.TYPE
        );
        final String selectedRealmType = randomFrom(platinumRealms);
        factories.put(selectedRealmType, config -> new DummyRealm(config));
        final LicensedFeature.Persistent feature = InternalRealms.getLicensedFeature(selectedRealmType);
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
        verify(licenseState, times(1)).isAllowed(feature);
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);

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

        verify(licenseState, times(2)).isAllowed(feature);
        verify(licenseState, times(1)).disableUsageTracking(feature, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);

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

        verify(licenseState, times(3)).isAllowed(feature);
        // this doesn't get called a second time because it didn't change
        verify(licenseState, times(1)).disableUsageTracking(feature, realmName);
        // this happened when the realm was allowed. Check it's still only 1 call
        verify(licenseState, times(1)).enableUsageTracking(feature, realmName);
    }

    public void testDisabledRealmsAreNotAdded() throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
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
            "Found realm "
                + (invalidRealmNames.size() == 1 ? "name" : "names")
                + " with reserved prefix [_]: ["
                + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ")
                + "]. "
                + "In a future major release, node will fail to start if any realm names start with reserved prefix."
        );
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
        public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
            listener.onResponse(AuthenticationResult.notHandled());
        }

        @Override
        public void lookupUser(String username, ActionListener<User> listener) {
            listener.onResponse(null);
        }
    }
}
