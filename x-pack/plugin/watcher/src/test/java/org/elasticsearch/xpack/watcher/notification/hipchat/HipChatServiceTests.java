/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.junit.Before;

import java.util.HashSet;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class HipChatServiceTests extends ESTestCase {
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testSingleAccountV1() throws Exception {
        String accountName = randomAlphaOfLength(10);
        String host = randomBoolean() ? null : "_host";
        int port = randomBoolean() ? -1 : randomIntBetween(300, 400);
        String defaultRoom = randomBoolean() ? null : "_r1, _r2";
        String defaultFrom = randomBoolean() ? null : "_from";
        HipChatMessage.Color defaultColor = randomBoolean() ? null : randomFrom(HipChatMessage.Color.values());
        HipChatMessage.Format defaultFormat = randomBoolean() ? null : randomFrom(HipChatMessage.Format.values());
        Boolean defaultNotify = randomBoolean() ? null : (Boolean) randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder()
                .put("xpack.notification.hipchat.account." + accountName + ".profile", HipChatAccount.Profile.V1.value())
                .put("xpack.notification.hipchat.account." + accountName + ".auth_token", "_token");
        if (host != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".host", host);
        }
        if (port > 0) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".port", port);
        }
        buildMessageDefaults(accountName, settingsBuilder, defaultRoom, null, defaultFrom, defaultColor, defaultFormat, defaultNotify);
        HipChatService service = new HipChatService(settingsBuilder.build(), httpClient,
                new ClusterSettings(settingsBuilder.build(), new HashSet<>(HipChatService.getSettings())));

        HipChatAccount account = service.getAccount(accountName);
        assertThat(account, notNullValue());
        assertThat(account.name, is(accountName));
        assertThat(account.authToken, is("_token"));
        assertThat(account.profile, is(HipChatAccount.Profile.V1));
        assertThat(account.httpClient, is(httpClient));
        assertThat(account.server, notNullValue());
        assertThat(account.server.host(), is(host != null ? host : HipChatServer.DEFAULT.host()));
        assertThat(account.server.port(), is(port > 0 ? port : HipChatServer.DEFAULT.port()));
        assertThat(account, instanceOf(V1Account.class));
        if (defaultRoom == null) {
            assertThat(((V1Account) account).defaults.rooms, nullValue());
        } else {
            assertThat(((V1Account) account).defaults.rooms, arrayContaining("_r1", "_r2"));
        }
        assertThat(((V1Account) account).defaults.from, is(defaultFrom));
        assertThat(((V1Account) account).defaults.color, is(defaultColor));
        assertThat(((V1Account) account).defaults.format, is(defaultFormat));
        assertThat(((V1Account) account).defaults.notify, is(defaultNotify));

        // with a single account defined, making sure that that account is set to the default one.
        assertThat(service.getAccount(null), sameInstance(account));
    }

    public void testSingleAccountIntegration() throws Exception {
        String accountName = randomAlphaOfLength(10);
        String host = randomBoolean() ? null : "_host";
        int port = randomBoolean() ? -1 : randomIntBetween(300, 400);
        String room = randomAlphaOfLength(10);
        String defaultFrom = randomBoolean() ? null : "_from";
        HipChatMessage.Color defaultColor = randomBoolean() ? null : randomFrom(HipChatMessage.Color.values());
        HipChatMessage.Format defaultFormat = randomBoolean() ? null : randomFrom(HipChatMessage.Format.values());
        Boolean defaultNotify = randomBoolean() ? null : (Boolean) randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder()
                .put("xpack.notification.hipchat.account." + accountName + ".profile",
                        HipChatAccount.Profile.INTEGRATION.value())
                .put("xpack.notification.hipchat.account." + accountName + ".auth_token", "_token")
                .put("xpack.notification.hipchat.account." + accountName + ".room", room);
        if (host != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".host", host);
        }
        if (port > 0) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".port", port);
        }
        buildMessageDefaults(accountName, settingsBuilder, null, null, defaultFrom, defaultColor, defaultFormat, defaultNotify);
        HipChatService service = new HipChatService(settingsBuilder.build(), httpClient,
                new ClusterSettings(settingsBuilder.build(), new HashSet<>(HipChatService.getSettings())));

        HipChatAccount account = service.getAccount(accountName);
        assertThat(account, notNullValue());
        assertThat(account.name, is(accountName));
        assertThat(account.authToken, is("_token"));
        assertThat(account.profile, is(HipChatAccount.Profile.INTEGRATION));
        assertThat(account.httpClient, is(httpClient));
        assertThat(account.server, notNullValue());
        assertThat(account.server.host(), is(host != null ? host : HipChatServer.DEFAULT.host()));
        assertThat(account.server.port(), is(port > 0 ? port : HipChatServer.DEFAULT.port()));
        assertThat(account, instanceOf(IntegrationAccount.class));
        assertThat(((IntegrationAccount) account).room, is(room));
        assertThat(((IntegrationAccount) account).defaults.color, is(defaultColor));
        assertThat(((IntegrationAccount) account).defaults.format, is(defaultFormat));
        assertThat(((IntegrationAccount) account).defaults.notify, is(defaultNotify));

        // with a single account defined, making sure that that account is set to the default one.
        assertThat(service.getAccount(null), sameInstance(account));
    }

    public void testSingleAccountIntegrationNoRoomSetting() throws Exception {
        String accountName = randomAlphaOfLength(10);
        Settings.Builder settingsBuilder = Settings.builder()
                .put("xpack.notification.hipchat.account." + accountName + ".profile",
                        HipChatAccount.Profile.INTEGRATION.value())
                .put("xpack.notification.hipchat.account." + accountName + ".auth_token", "_token");
        SettingsException e = expectThrows(SettingsException.class, () ->
            new HipChatService(settingsBuilder.build(), httpClient,
                new ClusterSettings(settingsBuilder.build(), new HashSet<>(HipChatService.getSettings()))).getAccount(null));
        assertThat(e.getMessage(), containsString("missing required [room] setting for [integration] account profile"));
    }

    public void testSingleAccountUser() throws Exception {
        String accountName = randomAlphaOfLength(10);
        String host = randomBoolean() ? null : "_host";
        int port = randomBoolean() ? -1 : randomIntBetween(300, 400);
        String defaultRoom = randomBoolean() ? null : "_r1, _r2";
        String defaultUser = randomBoolean() ? null : "_u1, _u2";
        HipChatMessage.Color defaultColor = randomBoolean() ? null : randomFrom(HipChatMessage.Color.values());
        HipChatMessage.Format defaultFormat = randomBoolean() ? null : randomFrom(HipChatMessage.Format.values());
        Boolean defaultNotify = randomBoolean() ? null : (Boolean) randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder()
                .put("xpack.notification.hipchat.account." + accountName + ".profile", HipChatAccount.Profile.USER.value())
                .put("xpack.notification.hipchat.account." + accountName + ".auth_token", "_token");
        if (host != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".host", host);
        }
        if (port > 0) {
            settingsBuilder.put("xpack.notification.hipchat.account." + accountName + ".port", port);
        }
        buildMessageDefaults(accountName, settingsBuilder, defaultRoom, defaultUser, null, defaultColor, defaultFormat, defaultNotify);
        HipChatService service = new HipChatService(settingsBuilder.build(), httpClient,
                new ClusterSettings(settingsBuilder.build(), new HashSet<>(HipChatService.getSettings())));

        HipChatAccount account = service.getAccount(accountName);
        assertThat(account, notNullValue());
        assertThat(account.name, is(accountName));
        assertThat(account.authToken, is("_token"));
        assertThat(account.profile, is(HipChatAccount.Profile.USER));
        assertThat(account.httpClient, is(httpClient));
        assertThat(account.server, notNullValue());
        assertThat(account.server.host(), is(host != null ? host : HipChatServer.DEFAULT.host()));
        assertThat(account.server.port(), is(port > 0 ? port : HipChatServer.DEFAULT.port()));
        assertThat(account, instanceOf(UserAccount.class));
        if (defaultRoom == null) {
            assertThat(((UserAccount) account).defaults.rooms, nullValue());
        } else {
            assertThat(((UserAccount) account).defaults.rooms, arrayContaining("_r1", "_r2"));
        }
        if (defaultUser == null) {
            assertThat(((UserAccount) account).defaults.users, nullValue());
        } else {
            assertThat(((UserAccount) account).defaults.users, arrayContaining("_u1", "_u2"));
        }
        assertThat(((UserAccount) account).defaults.color, is(defaultColor));
        assertThat(((UserAccount) account).defaults.format, is(defaultFormat));
        assertThat(((UserAccount) account).defaults.notify, is(defaultNotify));

        // with a single account defined, making sure that that account is set to the default one.
        assertThat(service.getAccount(null), sameInstance(account));
    }

    public void testMultipleAccounts() throws Exception {
        HipChatMessage.Color defaultColor = randomBoolean() ? null : randomFrom(HipChatMessage.Color.values());
        HipChatMessage.Format defaultFormat = randomBoolean() ? null : randomFrom(HipChatMessage.Format.values());
        Boolean defaultNotify = randomBoolean() ? null : (Boolean) randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder();
        String defaultAccount = "_a" + randomIntBetween(0, 4);
        settingsBuilder.put("xpack.notification.hipchat.default_account", defaultAccount);

        final boolean customGlobalServer = randomBoolean();
        if (customGlobalServer) {
            settingsBuilder.put("xpack.notification.hipchat.host", "_host_global");
            settingsBuilder.put("xpack.notification.hipchat.port", 299);
        }

        for (int i = 0; i < 5; i++) {
            String name = "_a" + i;
            String prefix = "xpack.notification.hipchat.account." + name;
            HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
            settingsBuilder.put(prefix + ".profile", profile);
            settingsBuilder.put(prefix + ".auth_token", "_token" + i);
            if (profile == HipChatAccount.Profile.INTEGRATION) {
                settingsBuilder.put(prefix + ".room", "_room" + i);
            }
            if (i % 2 == 0) {
                settingsBuilder.put(prefix + ".host", "_host" + i);
                settingsBuilder.put(prefix + ".port", 300 + i);
            }
            buildMessageDefaults(name, settingsBuilder, null, null, null, defaultColor, defaultFormat, defaultNotify);
        }

        HipChatService service = new HipChatService(settingsBuilder.build(), httpClient,
                new ClusterSettings(settingsBuilder.build(), new HashSet<>(HipChatService.getSettings())));

        for (int i = 0; i < 5; i++) {
            String name = "_a" + i;
            HipChatAccount account = service.getAccount(name);
            assertThat(account, notNullValue());
            assertThat(account.name, is(name));
            assertThat(account.authToken, is("_token" + i));
            assertThat(account.profile, notNullValue());
            if (account.profile == HipChatAccount.Profile.INTEGRATION) {
                assertThat(account, instanceOf(IntegrationAccount.class));
                assertThat(((IntegrationAccount) account).room, is("_room" + i));
            }
            assertThat(account.httpClient, is(httpClient));
            assertThat(account.server, notNullValue());
            if (i % 2 == 0) {
                assertThat(account.server.host(), is("_host" + i));
                assertThat(account.server.port(), is(300 + i));
            } else if (customGlobalServer) {
                assertThat(account.server.host(), is("_host_global"));
                assertThat(account.server.port(), is(299));
            } else {
                assertThat(account.server.host(), is(HipChatServer.DEFAULT.host()));
                assertThat(account.server.port(), is(HipChatServer.DEFAULT.port()));
            }
        }

        assertThat(service.getAccount(null), sameInstance(service.getAccount(defaultAccount)));
    }

    private void buildMessageDefaults(String account, Settings.Builder settingsBuilder, String room, String user, String from,
                                      HipChatMessage.Color color, HipChatMessage.Format format, Boolean notify) {
        if (room != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.room", room);
        }
        if (user != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.user", user);
        }
        if (from != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.from", from);
        }
        if (color != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.color", color.value());
        }
        if (format != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.format", format);
        }
        if (notify != null) {
            settingsBuilder.put("xpack.notification.hipchat.account." + account + ".message_defaults.notify", notify);
        }
    }
}
