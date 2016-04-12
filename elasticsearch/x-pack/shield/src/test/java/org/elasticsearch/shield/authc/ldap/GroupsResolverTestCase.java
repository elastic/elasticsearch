/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

public abstract class GroupsResolverTestCase extends ESTestCase {

    protected LDAPConnection ldapConnection;

    protected abstract String ldapUrl();

    protected abstract String bindDN();

    protected abstract String bindPassword();

    @Before
    public void setUpLdapConnection() throws Exception {
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        boolean useGlobalSSL = randomBoolean();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.security.ssl.keystore.path", keystore)
                    .put("xpack.security.ssl.keystore.password", "changeit");
        } else {
            builder.put(Global.AUTO_GENERATE_SSL_SETTING.getKey(), false);
        }
        Settings settings = builder.build();
        Environment env = new Environment(settings);
        ClientSSLService clientSSLService = new ClientSSLService(settings, new Global(settings));
        clientSSLService.setEnvironment(env);

        LDAPURL ldapurl = new LDAPURL(ldapUrl());
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setFollowReferrals(true);
        options.setAutoReconnect(true);
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

        ldapConnection = new LDAPConnection(clientSSLService.sslSocketFactory(connectionSettings), options, ldapurl.getHost(),
                ldapurl.getPort(), bindDN(), bindPassword());
    }

    @After
    public void tearDownLdapConnection() throws Exception {
        ldapConnection.close();
    }
}
