/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtRealmTests extends JwtTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private Settings defaultGlobalSettings;
    private SSLService sslService;
    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        this.threadPool = new TestThreadPool("JWT realm tests");
        this.resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, this.threadPool);
        this.defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        this.sslService = new SSLService(TestEnvironment.newEnvironment(this.defaultGlobalSettings));
        this.licenseState = mock(MockLicenseState.class);
        when(this.licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    public void testRealm() throws Exception {
        final String principal = "principal" + randomIntBetween(10, 99);

        final String authzName = randomBoolean() ? "" : "other" + randomIntBetween(10, 99);

        // If delegatedAuthz is randomly off, authc JWT realm needs UserRoleMapper
        // If delegatedAuthz is randomly on, authc JWT realm settings needs authz realm setting to point to mocked realm name.
        final String authcName = "jwt" + randomIntBetween(10, 99);
        final Settings authcSettings = super.getAllRealmSettings(authcName).put(
            RealmSettings.getFullSettingKey(authcName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
            authzName
        ).build();
        final RealmConfig authcConfig = super.buildRealmConfig(JwtRealmSettings.TYPE, authcName, authcSettings, Integer.valueOf(0));
        final UserRoleMapper authcUserRoleMapper = super.buildRoleMapper(
            principal,
            Strings.hasText(authzName) ? Collections.emptySet() : Set.of("authcRole1, authcRole2")
        );
        final JwtRealm authcRealm = new JwtRealm(
            authcConfig,
            this.threadPool,
            this.sslService,
            authcUserRoleMapper,
            this.resourceWatcherService
        );

        final List<Realm> realms = new ArrayList<>(); // authc only, or authc and authz
        realms.add(authcRealm);

        if (Strings.hasText(authzName)) {
            final Settings authzSettings = Settings.builder().build();
            final RealmConfig authzConfig = this.buildRealmConfig("mock", authzName, authzSettings, Integer.valueOf(1));
            final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
            authzRealm.registerUser(
                new User(
                    principal,
                    new String[] { "authzRole1", "authzRole2" },
                    "Full Name" + randomIntBetween(10, 99),
                    "emailusername" + randomIntBetween(10, 99) + "@example.com",
                    Collections.singletonMap("is_lookup", true),
                    true
                )
            );
            realms.add(authzRealm);
        }

        for (final Realm realm : realms) {
            realm.initialize(realms, this.licenseState);
        }
    }
}
