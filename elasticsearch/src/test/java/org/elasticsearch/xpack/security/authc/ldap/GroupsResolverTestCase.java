/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.LDAPURL;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;

public abstract class GroupsResolverTestCase extends ESTestCase {

    LDAPConnection ldapConnection;

    protected abstract String ldapUrl();

    protected abstract String bindDN();

    protected abstract String bindPassword();

    @Before
    public void setUpLdapConnection() throws Exception {
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        boolean useGlobalSSL = randomBoolean();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.ssl.keystore.path", keystore)
                    .put("xpack.ssl.keystore.password", "changeit");

            // fake realm to load config with certificate verification mode
            builder.put("xpack.security.authc.realms.bar.ssl.keystore.path", keystore);
            builder.put("xpack.security.authc.realms.bar.ssl.keystore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        } else {
            // fake realms so ssl will get loaded
            builder.put("xpack.security.authc.realms.foo.ssl.keystore.path", keystore);
            builder.put("xpack.security.authc.realms.foo.ssl.keystore.password", "changeit");
            builder.put("xpack.security.authc.realms.foo.ssl.verification_mode", VerificationMode.FULL);
            builder.put("xpack.security.authc.realms.bar.ssl.keystore.path", keystore);
            builder.put("xpack.security.authc.realms.bar.ssl.keystore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        SSLService sslService = new SSLService(settings, env);

        LDAPURL ldapurl = new LDAPURL(ldapUrl());
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setFollowReferrals(true);
        options.setAllowConcurrentSocketFactoryUse(true);
        options.setConnectTimeoutMillis(Math.toIntExact(SessionFactory.TIMEOUT_DEFAULT.millis()));
        options.setResponseTimeoutMillis(SessionFactory.TIMEOUT_DEFAULT.millis());

        Settings connectionSettings;
        if (useGlobalSSL) {
            connectionSettings = Settings.EMPTY;
        } else {
            connectionSettings = Settings.builder().put("keystore.path", keystore)
                    .put("keystore.password", "changeit").build();
        }
        ldapConnection = LdapUtils.privilegedConnect(() -> new LDAPConnection(sslService.sslSocketFactory(connectionSettings), options,
                ldapurl.getHost(), ldapurl.getPort(), bindDN(), bindPassword()));
    }

    @After
    public void tearDownLdapConnection() throws Exception {
        if (ldapConnection != null) {
            ldapConnection.close();
        }
    }

    protected static List<String> resolveBlocking(GroupsResolver resolver, LDAPInterface ldapConnection, String dn, TimeValue timeLimit,
                                                  Logger logger, Collection<Attribute> attributes) {
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        resolver.resolve(ldapConnection, dn, timeLimit, logger, attributes, future);
        return future.actionGet();
    }
}
