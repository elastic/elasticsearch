/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class RealmsTests extends ElasticsearchTestCase {

    private Map<String, Realm.Factory> factories;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();
        factories.put("esusers", new DummyRealm.Factory("esusers", true));
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            DummyRealm.Factory factory = new DummyRealm.Factory("type_" + i, rarely());
            factories.put("type_" + i, factory);
        }
    }

    @Test
    public void testWithSettings() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
        Realms realms = new Realms(settings, env, factories);
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

    @Test(expected = ShieldSettingsException.class)
    public void testWithSettings_WithMultipleInternalRealmsOfSameType() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.realms.realm_1.type", ESUsersRealm.TYPE)
                .put("shield.authc.realms.realm_1.order", 0)
                .put("shield.authc.realms.realm_2.type", ESUsersRealm.TYPE)
                .put("shield.authc.realms.realm_2.order", 1)
                .build();
        Environment env = new Environment(settings);
        new Realms(settings, env, factories).start();
    }

    @Test
    public void testWithEmptySettings() throws Exception {
        Realms realms = new Realms(ImmutableSettings.EMPTY, new Environment(ImmutableSettings.EMPTY), factories);
        realms.start();
        Iterator<Realm> iter = realms.iterator();
        assertThat(iter.hasNext(), is(true));
        Realm realm = iter.next();
        assertThat(realm, notNullValue());
        assertThat(realm.type(), equalTo(ESUsersRealm.TYPE));
        assertThat(realm.name(), equalTo("default_" + ESUsersRealm.TYPE));
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void testDisabledRealmsAreNotAdded() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
        Realms realms = new Realms(settings, env, factories);
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
                return type().equals("esusers") ? new DummyRealm("esusers", new RealmConfig(name)) : null;
            }
        }
    }
}
