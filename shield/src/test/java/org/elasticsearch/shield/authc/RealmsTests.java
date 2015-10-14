/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportMessage;
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
    private ShieldSettingsFilter settingsFilter;
    private ShieldLicenseState shieldLicenseState;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();
        factories.put("esusers", new DummyRealm.Factory("esusers", true));
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            DummyRealm.Factory factory = new DummyRealm.Factory("type_" + i, rarely());
            factories.put("type_" + i, factory);
        }
        settingsFilter = mock(ShieldSettingsFilter.class);
        shieldLicenseState = mock(ShieldLicenseState.class);
        when(shieldLicenseState.customRealmsEnabled()).thenReturn(true);
    }

    public void testWithSettings() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 1);
        for (int i = 0; i < factories.size() - 1; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, getRandom());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 1; i++) {
            builder.put("shield.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("shield.authc.realms.realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, settingsFilter, shieldLicenseState);
        realms.start();
        int i = 0;
        for (Realm realm : realms) {
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }
    }

    public void testWithSettingsWithMultipleInternalRealmsOfSameType() throws Exception {
        Settings settings = Settings.builder()
                .put("shield.authc.realms.realm_1.type", ESUsersRealm.TYPE)
                .put("shield.authc.realms.realm_1.order", 0)
                .put("shield.authc.realms.realm_2.type", ESUsersRealm.TYPE)
                .put("shield.authc.realms.realm_2.order", 1)
                .put("path.home", createTempDir())
                .build();
        Environment env = new Environment(settings);
        try {
            new Realms(settings, env, factories, settingsFilter, shieldLicenseState).start();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("multiple [esusers] realms are configured"));
        }
    }

    public void testWithEmptySettings() throws Exception {
        Realms realms = new Realms(Settings.EMPTY, new Environment(Settings.builder().put("path.home", createTempDir()).build()),
                factories, settingsFilter, shieldLicenseState);
        realms.start();
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, notNullValue());
        assertThat(realm.type(), equalTo(ESUsersRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + ESUsersRealm.TYPE));
        assertThat(iter.hasNext(), is(false));
    }

    public void testUnlicensedWithOnlyCustomRealms() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 1);
        for (int i = 0; i < factories.size() - 1; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, getRandom());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 1; i++) {
            builder.put("shield.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("shield.authc.realms.realm_" + i + ".order", orders.get(i));
            orderToIndex.put(orders.get(i), i);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, settingsFilter, shieldLicenseState);
        realms.start();
        int i = 0;
        // this is the iterator when licensed
        for (Realm realm : realms) {
            assertThat(realm.order(), equalTo(i));
            int index = orderToIndex.get(i);
            assertThat(realm.type(), equalTo("type_" + index));
            assertThat(realm.name(), equalTo("realm_" + index));
            i++;
        }

        i = 0;
        when(shieldLicenseState.customRealmsEnabled()).thenReturn(false);
        for (Realm realm : realms) {
            assertThat(realm.type, is(ESUsersRealm.TYPE));
            i++;
        }
        assertThat(i, is(1));
    }

    public void testUnlicensedWithInternalRealms() throws Exception {
        factories.put(LdapRealm.TYPE, new DummyRealm.Factory(LdapRealm.TYPE, false));
        assertThat(factories.get("type_0"), notNullValue());
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir())
                .put("shield.authc.realms.foo.type", "ldap")
                .put("shield.authc.realms.foo.order", "0")
                .put("shield.authc.realms.custom.type", "type_0")
                .put("shield.authc.realms.custom.order", "1");
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, settingsFilter, shieldLicenseState);
        realms.start();
        int i = 0;
        // this is the iterator when licensed
        List<String> types = new ArrayList<>();
        for (Realm realm : realms) {
            i++;
            types.add(realm.type());
        }
        assertThat(types, contains("ldap", "type_0"));

        i = 0;
        when(shieldLicenseState.customRealmsEnabled()).thenReturn(false);
        for (Realm realm : realms) {
            assertThat(realm.type, is("ldap"));
            i++;
        }
        assertThat(i, is(1));
    }

    public void testDisabledRealmsAreNotAdded() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("path.home", createTempDir());
        List<Integer> orders = new ArrayList<>(factories.size() - 1);
        for (int i = 0; i < factories.size() - 1; i++) {
            orders.add(i);
        }
        Collections.shuffle(orders, getRandom());
        Map<Integer, Integer> orderToIndex = new HashMap<>();
        for (int i = 0; i < factories.size() - 1; i++) {
            builder.put("shield.authc.realms.realm_" + i + ".type", "type_" + i);
            builder.put("shield.authc.realms.realm_" + i + ".order", orders.get(i));
            boolean enabled = randomBoolean();
            builder.put("shield.authc.realms.realm_" + i + ".enabled", enabled);
            if (enabled) {
                orderToIndex.put(orders.get(i), i);
                logger.error("put [{}] -> [{}]", orders.get(i), i);
            }
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        Realms realms = new Realms(settings, env, factories, mock(ShieldSettingsFilter.class), shieldLicenseState);
        realms.start();
        Iterator<Realm> iterator = realms.iterator();

        int count = 0;
        while (iterator.hasNext()) {
            Realm realm = iterator.next();
            Integer index = orderToIndex.get(realm.order());
            if (index == null) {
                // Default realm is inserted when factories size is 1 and enabled is false
                assertThat(realm.type(), equalTo(ESUsersRealm.TYPE));
                assertThat(realm.name(), equalTo("default_" + ESUsersRealm.TYPE));
                assertThat(iterator.hasNext(), is(false));
            } else {
                assertThat(realm.type(), equalTo("type_" + index));
                assertThat(realm.name(), equalTo("realm_" + index));
                assertThat(settings.getAsBoolean("shield.authc.realms.realm_" + index + ".enabled", true), equalTo(Boolean.TRUE));
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
        public AuthenticationToken token(RestRequest request) {
            return null;
        }

        @Override
        public AuthenticationToken token(TransportMessage message) {
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
                if (type().equals("esusers")) {
                    return new DummyRealm("esusers", new RealmConfig(name, Settings.EMPTY, Settings.builder().put("path.home", createTempDir()).build()));
                }
                return null;
            }
        }
    }
}
