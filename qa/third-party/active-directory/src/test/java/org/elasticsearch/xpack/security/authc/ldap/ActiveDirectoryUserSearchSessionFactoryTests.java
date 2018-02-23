/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;

public class ActiveDirectoryUserSearchSessionFactoryTests extends LdapTestCase {

    private SSLService sslService;
    private Settings globalSettings;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        Path keystore = getDataPath("support/ADtrust.jks");
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */

        globalSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.truststore.path", keystore)
                .setSecureSettings(newSecureSettings("xpack.ssl.truststore.secure_password", "changeit"))
                .build();
        sslService = new SSLService(globalSettings, env);
        threadPool = new TestThreadPool("LdapUserSearchSessionFactoryTests");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    @Network
    @SuppressWarnings("unchecked")
    public void testUserSearchWithActiveDirectory() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userSearchBase = "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(
                        new String[] { ActiveDirectorySessionFactoryTests.AD_LDAP_URL },
                        Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE, null,
                        true))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "ironman@ad.test.elasticsearch.com")
                .put("bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                .put("user_search.filter", "(cn={0})")
                .put("user_search.pool.enabled", randomBoolean())
                .build();
        Settings.Builder builder = Settings.builder()
                .put(globalSettings);
        settings.keySet().forEach(k -> {
            builder.copy("xpack.security.authc.realms.ldap." + k, k, settings);

        });
        Settings fullSettings = builder.build();
        sslService = new SSLService(fullSettings, TestEnvironment.newEnvironment(fullSettings));
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        LdapUserSearchSessionFactory sessionFactory = getLdapUserSearchSessionFactory(config, sslService, threadPool);

        String user = "Bruce Banner";
        try {
            //auth
            try (LdapSession ldap = session(sessionFactory, user, new SecureString(ActiveDirectorySessionFactoryTests.PASSWORD))) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists")));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists")));
            }
        } finally {
            sessionFactory.close();
        }
    }

    static LdapUserSearchSessionFactory getLdapUserSearchSessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool)
            throws LDAPException {
        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService, threadPool);
        if (sessionFactory.getConnectionPool() != null) {
            // don't use this in production
            // used here to catch bugs that might get masked by an automatic retry
            sessionFactory.getConnectionPool().setRetryFailedOperationsDueToInvalidConnections(false);
        }
        return sessionFactory;
    }

    private MockSecureSettings newSecureSettings(String key, String value) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(key, value);
        return secureSettings;
    }
}
