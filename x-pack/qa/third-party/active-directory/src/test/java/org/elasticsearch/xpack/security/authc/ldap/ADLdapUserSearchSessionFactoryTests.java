/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.security.authc.ldap.LdapUserSearchSessionFactoryTests.getLdapUserSearchSessionFactory;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class ADLdapUserSearchSessionFactoryTests extends AbstractActiveDirectoryTestCase {

    private SSLService sslService;
    private Settings globalSettings;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        Path certPath = getDataPath("support/smb_ca.crt");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use an SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */

        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.ldap.ad-as-ldap-test.ssl.certificate_authorities", certPath)
            .build();
        Environment env = TestEnvironment.newEnvironment(globalSettings);
        sslService = new SSLService(env);
        threadPool = new TestThreadPool("ADLdapUserSearchSessionFactoryTests");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testUserSearchWithActiveDirectory() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ad-as-ldap-test");
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userSearchBase = "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put("url", ActiveDirectorySessionFactoryTests.AD_LDAP_URL)
                .put("group_search.base_dn", groupSearchBase)
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "ironman@ad.test.elasticsearch.com")
                .put("bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                .put("user_search.filter", "(cn={0})")
                .put("user_search.pool.enabled", randomBoolean())
                .put("follow_referrals", ActiveDirectorySessionFactoryTests.FOLLOW_REFERRALS)
                .put("order", 0)
                .build();
        Settings.Builder builder = Settings.builder()
                .put(globalSettings);
        settings.keySet().forEach(k -> {
            builder.copy("xpack.security.authc.realms.ldap.ad-as-ldap-test." + k, k, settings);
        });
        Settings fullSettings = builder.build();
        sslService = new SSLService(TestEnvironment.newEnvironment(fullSettings));
        RealmConfig config = new RealmConfig(realmIdentifier, fullSettings,
                TestEnvironment.newEnvironment(fullSettings), new ThreadContext(fullSettings));
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

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    private LdapSession session(SessionFactory factory, String username, SecureString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    private List<String> groups(LdapSession ldapSession) {
        Objects.requireNonNull(ldapSession);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }

    private LdapSession unauthenticatedSession(SessionFactory factory, String username) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.unauthenticatedSession(username, future);
        return future.actionGet();
    }
}
