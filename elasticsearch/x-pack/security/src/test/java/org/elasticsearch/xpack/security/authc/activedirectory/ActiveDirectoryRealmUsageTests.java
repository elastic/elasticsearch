/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.activedirectory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.test.junit.annotations.Network;

import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

@Network
public class ActiveDirectoryRealmUsageTests extends AbstractActiveDirectoryIntegTests {

    public void testUsageStats() throws Exception {
        String loadBalanceType = randomFrom("failover", "round_robin");
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Bruce Banner, CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.BASE, false))
                .put("load_balance.type", loadBalanceType)
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, mock(DnRoleMapper.class));

        Map<String, Object> stats = realm.usageStats();
        assertThat(stats, is(notNullValue()));
        assertThat(stats, hasEntry("type", "active_directory"));
        assertThat(stats, hasEntry("name", "ad-test"));
        assertThat(stats, hasEntry("order", realm.order()));
        assertThat(stats, hasEntry("size", "tiny"));
        assertThat(stats, hasEntry("ssl", true));
        assertThat(stats, hasEntry("load_balance_type", loadBalanceType));
    }
}
