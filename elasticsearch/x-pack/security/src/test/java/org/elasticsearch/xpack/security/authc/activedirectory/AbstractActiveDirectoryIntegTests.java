/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.activedirectory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import java.nio.file.Path;

@Network
public class AbstractActiveDirectoryIntegTests extends ESTestCase {

    public static final String AD_LDAP_URL = "ldaps://54.213.145.20:636";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String AD_DOMAIN = "ad.test.elasticsearch.com";

    protected ClientSSLService clientSSLService;
    protected Settings globalSettings;
    protected boolean useGlobalSSL;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        useGlobalSSL = randomBoolean();
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.security.ssl.keystore.path", keystore)
                    .put("xpack.security.ssl.keystore.password", "changeit");
        }
        globalSettings = builder.build();
        Environment environment = new Environment(globalSettings);
        clientSSLService = new ClientSSLService(globalSettings, environment, new Global(globalSettings));
    }

    Settings buildAdSettings(String ldapUrl, String adDomainName, String userSearchDN, LdapSearchScope scope,
                                           boolean hostnameVerification) {
        Settings.Builder builder = Settings.builder()
                .putArray(ActiveDirectorySessionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_BASEDN_SETTING, userSearchDN)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_SCOPE_SETTING, scope)
                .put(ActiveDirectorySessionFactory.HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        if (useGlobalSSL == false) {
            builder.put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit");
        }
        return builder.build();
    }
}
