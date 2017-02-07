/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;
import org.junit.Before;

import java.nio.file.Path;

@Network
public class AbstractActiveDirectoryIntegTests extends ESTestCase {

    public static final String AD_LDAP_URL = "ldaps://54.213.145.20:636";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String AD_DOMAIN = "ad.test.elasticsearch.com";

    protected SSLService sslService;
    protected Settings globalSettings;
    protected boolean useGlobalSSL;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        useGlobalSSL = randomBoolean();
        Path truststore = getDataPath("../ldap/support/ldaptrust.jks");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.ssl.truststore.path", truststore)
                    .put("xpack.ssl.truststore.password", "changeit");

            // fake realm to load config with certificate verification mode
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        } else {
            // fake realms so ssl will get loaded
            builder.put("xpack.security.authc.realms.foo.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.foo.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.foo.ssl.verification_mode", VerificationMode.FULL);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        }
        globalSettings = builder.build();
        Environment environment = new Environment(globalSettings);
        sslService = new SSLService(globalSettings, environment);
    }

    Settings buildAdSettings(String ldapUrl, String adDomainName, String userSearchDN, LdapSearchScope scope,
                                           boolean hostnameVerification) {
        Settings.Builder builder = Settings.builder()
                .putArray(ActiveDirectorySessionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_BASEDN_SETTING, userSearchDN)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_SCOPE_SETTING, scope);
        if (randomBoolean()) {
            builder.put("ssl.verification_mode", hostnameVerification ? VerificationMode.FULL : VerificationMode.CERTIFICATE);
        } else {
            builder.put(ActiveDirectorySessionFactory.HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        }
        if (useGlobalSSL == false) {
            builder.put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit");
        }
        return builder.build();
    }
}
