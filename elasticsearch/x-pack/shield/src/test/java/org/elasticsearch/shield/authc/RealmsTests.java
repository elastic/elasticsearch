/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.SecurityLicenseState.EnabledRealmType;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.authc.esnative.NativeRealm;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class RealmsTests extends ESTestCase {
    private Map<String, Realm.Factory> factories;
    private SecurityLicenseState shieldLicenseState;
    private ReservedRealm reservedRealm;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();
        factories.put(FileRealm.TYPE, new DummyRealm.Factory(FileRealm.TYPE, true));
        factories.put(NativeRealm.TYPE, new DummyRealm.Factory(NativeRealm.TYPE, true));
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            DummyRealm.Factory factory = new DummyRealm.Factory("type_" + i, rarely());
            factories.put("type_" + i, factory);
        }
        shieldLicenseState = mock(SecurityLicenseState.class);
        reservedRealm = mock(ReservedRealm.class);
        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.ALL);
    }

    public void testWithSettings() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 2);
        for (int i = 0; i < factories.size() - 2; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 2; i++) {
            builder.put("xpack.security.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("xpack.security.authc.realms.realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, shieldLicenseState, reservedRealm);
        realms.start();

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
    }

    public void testWithSettingsWithMultipleInternalRealmsOfSameType() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.realm_1.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.realm_1.order", 0)
                .put("xpack.security.authc.realms.realm_2.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.realm_2.order", 1)
                .put("path.home", createTempDir())
                .build();
        Environment env = new Environment(settings);
        try {
            new Realms(settings, env, factories, shieldLicenseState, reservedRealm).start();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("multiple [file] realms are configured"));
        }
    }

    public void testWithEmptySettings() throws Exception {
        Realms realms = new Realms(Settings.EMPTY, new Environment(Settings.builder().put("path.home", createTempDir()).build()),
                factories, shieldLicenseState, reservedRealm);
        realms.start();
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealm.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealm.TYPE));
        assertThat(iter.hasNext(), is(false));
    }

    public void testUnlicensedWithOnlyCustomRealms() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 2);
        for (int i = 0; i < factories.size() - 2; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 2; i++) {
            builder.put("xpack.security.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("xpack.security.authc.realms.realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, shieldLicenseState, reservedRealm);
        realms.start();

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

        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.DEFAULT);

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealm.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealm.TYPE));
        assertThat(iter.hasNext(), is(false));

        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.NATIVE);

        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealm.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealm.TYPE));
        assertThat(iter.hasNext(), is(false));
    }

    public void testUnlicensedWithInternalRealms() throws Exception {
        factories.put(LdapRealm.TYPE, new DummyRealm.Factory(LdapRealm.TYPE, false));
        assertThat(factories.get("type_0"), notNullValue());
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.security.authc.realms.foo.type", "ldap")
                .put("xpack.security.authc.realms.foo.order", "0")
                .put("xpack.security.authc.realms.custom.type", "type_0")
                .put("xpack.security.authc.realms.custom.order", "1");
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, shieldLicenseState, reservedRealm);
        realms.start();
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

        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.DEFAULT);
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        i = 0;
        while (iter.hasNext()) {
            realm = iter.next();
            assertThat(realm.type, is("ldap"));
            i++;
        }
        assertThat(i, is(1));

        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.NATIVE);
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(FileRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + FileRealm.TYPE));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), equalTo(NativeRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + NativeRealm.TYPE));
        assertThat(iter.hasNext(), is(false));
    }

    public void testUnlicensedWithNativeRealms() throws Exception {
        factories.put(LdapRealm.TYPE, new DummyRealm.Factory(LdapRealm.TYPE, false));
        final String type = randomFrom(FileRealm.TYPE, NativeRealm.TYPE);
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.security.authc.realms.foo.type", "ldap")
                .put("xpack.security.authc.realms.foo.order", "0")
                .put("xpack.security.authc.realms.native.type", type)
                .put("xpack.security.authc.realms.native.order", "1");
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, shieldLicenseState, reservedRealm);
        realms.start();
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

        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.NATIVE);
        iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm, is(reservedRealm));
        assertThat(iter.hasNext(), is(true));
        realm = iter.next();
        assertThat(realm.type(), is(type));
        assertThat(iter.hasNext(), is(false));
    }

    public void testDisabledRealmsAreNotAdded() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 2);
        for (int i = 0; i < factories.size() - 2; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, random());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 2; i++) {
            builder.put("xpack.security.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("xpack.security.authc.realms.realm_" + i + ".order", orders.get(i));
            boolean enabled = randomBoolean();
            builder.put("xpack.security.authc.realms.realm_" + i + ".enabled", enabled);
            if (enabled) {
                orderToIndex.put(orders.get(i), i);
                logger.error("put [{}] -> [{}]", orders.get(i), i);
            }
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, shieldLicenseState, reservedRealm);
        realms.start();
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
                assertThat(realm.type(), equalTo(FileRealm.TYPE));
                assertThat(realm.name(), equalTo("default_" + FileRealm.TYPE));
                assertThat(iterator.hasNext(), is(true));
                realm = iterator.next();
                assertThat(realm.type(), equalTo(NativeRealm.TYPE));
                assertThat(realm.name(), equalTo("default_" + NativeRealm.TYPE));
                assertThat(iterator.hasNext(), is(false));
            } else {
                assertThat(realm.type(), equalTo("type_" + index));
                assertThat(realm.name(), equalTo("realm_" + index));
                assertThat(settings.getAsBoolean("xpack.security.authc.realms.realm_" + index + ".enabled", true), equalTo(Boolean.TRUE));
                count++;
            }
        }

        assertThat(count, equalTo(orderToIndex.size()));
    }

    static class DummyRealm extends Realm {

        public DummyRealm(String type, RealmConfig config) {
            super(type, config);
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
        public User authenticate(AuthenticationToken token) {
            return null;
        }

        @Override
        public User lookupUser(String username) {
            return null;
        }

        @Override
        public boolean userLookupSupported() {
            return false;
        }

        static class Factory extends Realm.Factory<DummyRealm> {

            public Factory(String type, boolean internal) {
                super(type, internal);
            }

            @Override
            public DummyRealm create(RealmConfig config) {
                return new DummyRealm(type(), config);
            }

            @Override
            public DummyRealm createDefault(String name) {
                if (type().equals(NativeRealm.TYPE) || type().equals(FileRealm.TYPE)) {
                    return new DummyRealm(type(), new RealmConfig(name, Settings.EMPTY,
                            Settings.builder().put("path.home", createTempDir()).build()));
                }
                return null;
            }
        }
    }
}
