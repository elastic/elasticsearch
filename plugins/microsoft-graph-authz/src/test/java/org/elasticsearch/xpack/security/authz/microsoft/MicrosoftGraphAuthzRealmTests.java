/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class MicrosoftGraphAuthzRealmTests extends ESTestCase {

    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testLookupUser() {
        final var realmName = "ms-graph";
        final var realmId = new RealmConfig.RealmIdentifier(MicrosoftGraphAuthzRealmSettings.REALM_TYPE, realmName);

        final var roleMapper = mock(UserRoleMapper.class);
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET), "client-secret");
        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_ID), "client-id")
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.TENANT_ID), "tenant-id")
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.ACCESS_TOKEN_HOST), "https://localhost:12345")
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.API_HOST), "https://localhost:12345/v1.0")
            .setSecureSettings(secureSettings)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser("principal", future);
        final var user = future.actionGet();
        assertThat(user.principal(), equalTo("principal"));
        assertThat(user.email(), equalTo("email"));
        assertThat(user.roles(), arrayContaining("role1"));
    }

    public void testHandleGetAccessTokenError() {
        fail();
    }

    public void testHandleGetUserPropertiesError() {
        fail();
    }

    public void testHandleGetGroupMembershipError() {
        fail();
    }

    public void testGroupMembershipPagination() {
        fail();
    }

    public void testLicenseCheck() {
        fail();
    }
}
