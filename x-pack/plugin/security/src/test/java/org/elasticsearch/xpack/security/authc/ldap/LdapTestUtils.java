/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPURL;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;

import java.util.List;

public class LdapTestUtils {

    private LdapTestUtils() {
        // Utility class
    }

    public static LDAPConnection openConnection(String url, String bindDN, String bindPassword,
                                                List<String> certificateAuthorities) throws Exception {
        Settings.Builder builder = Settings.builder().put("path.home", LuceneTestCase.createTempDir());
        boolean useGlobalSSL = ESTestCase.randomBoolean();
        if (useGlobalSSL) {
            builder.putList("xpack.ssl.certificate_authorities", certificateAuthorities);
            // fake realm to load config with certificate verification mode
            builder.putList("xpack.security.authc.realms.bar.ssl.certificate_authorities", certificateAuthorities);
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        } else {
            // fake realms so ssl will get loaded
            builder.putList("xpack.security.authc.realms.foo.ssl.certificate_authorities", certificateAuthorities);
            builder.put("xpack.security.authc.realms.foo.ssl.verification_mode", VerificationMode.FULL);
            builder.putList("xpack.security.authc.realms.bar.ssl.certificate_authorities", certificateAuthorities);
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        }
        Settings settings = builder.build();
        Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(settings, env);
        LDAPURL ldapurl = new LDAPURL(url);
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setFollowReferrals(true);
        options.setAllowConcurrentSocketFactoryUse(true);
        options.setConnectTimeoutMillis(Math.toIntExact(SessionFactorySettings.TIMEOUT_DEFAULT.millis()));
        options.setResponseTimeoutMillis(SessionFactorySettings.TIMEOUT_DEFAULT.millis());
        Settings connectionSettings;
        if (useGlobalSSL) {
            connectionSettings = Settings.EMPTY;
        } else {
            connectionSettings = Settings.builder().putList("certificate_authorities", certificateAuthorities)
                .build();
        }
        return LdapUtils.privilegedConnect(() -> new LDAPConnection(sslService.sslSocketFactory(connectionSettings), options,
            ldapurl.getHost(), ldapurl.getPort(), bindDN, bindPassword));
    }
}
