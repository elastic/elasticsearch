/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

@Network
public class OpenLdapTests extends ElasticsearchTestCase {
    public static final String OPEN_LDAP_URL = "ldaps://54.200.235.244:636";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String SETTINGS_PREFIX = LdapRealm.class.getPackage().getName().substring("com.elasticsearch.".length()) + '.';

    @BeforeClass
    public static void setTrustStore() throws URISyntaxException {
        File filename = new File(LdapConnectionTests.class.getResource("../support/ldap/ldaptrust.jks").toURI()).getAbsoluteFile();
        LdapSslSocketFactory.init(new SSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore", filename)
                .put("shield.ssl.keystore_password", "changeit")
                .build()));
    }

    @AfterClass
    public static void clearTrustStore() {
        LdapSslSocketFactory.clear();
    }

    @Test
    public void test_standardLdapConnection_uid(){
        //openldap does not use cn as naming attributes by default

        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        boolean isSubTreeSearch = true;
        LdapConnectionFactory connectionFactory = new LdapConnectionFactory(
                LdapConnectionTests.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, isSubTreeSearch));

        String[] users = new String[]{"blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor"};
        for(String user: users) {
            LdapConnection ldap = connectionFactory.open(user, SecuredStringTests.build(PASSWORD));
            assertThat(ldap.groups(), hasItem(containsString("Avengers")));
            ldap.close();
        }
    }

}
