/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IntegrationAccountTests extends ESTestCase {

    public void testSettings() throws Exception {
        String accountName = "_name";

        Settings.Builder sb = Settings.builder();

        String authToken = randomAlphaOfLength(50);
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, authToken);

        String host = HipChatServer.DEFAULT.host();
        if (randomBoolean()) {
            host = randomAlphaOfLength(10);
            sb.put("host", host);
        }
        int port = HipChatServer.DEFAULT.port();
        if (randomBoolean()) {
            port = randomIntBetween(300, 400);
            sb.put("port", port);
        }

        String room = randomAlphaOfLength(10);
        sb.put(IntegrationAccount.ROOM_SETTING, room);

        HipChatMessage.Format defaultFormat = null;
        if (randomBoolean()) {
            defaultFormat = randomFrom(HipChatMessage.Format.values());
            sb.put(HipChatAccount.DEFAULT_FORMAT_SETTING, defaultFormat);
        }
        HipChatMessage.Color defaultColor = null;
        if (randomBoolean()) {
            defaultColor = randomFrom(HipChatMessage.Color.values());
            sb.put(HipChatAccount.DEFAULT_COLOR_SETTING, defaultColor);
        }
        Boolean defaultNotify = null;
        if (randomBoolean()) {
            defaultNotify = randomBoolean();
            sb.put(HipChatAccount.DEFAULT_NOTIFY_SETTING, defaultNotify);
        }
        Settings settings = sb.build();

        IntegrationAccount account = new IntegrationAccount(accountName, settings, HipChatServer.DEFAULT, mock(HttpClient.class),
                mock(Logger.class));

        assertThat(account.profile, is(HipChatAccount.Profile.INTEGRATION));
        assertThat(account.name, equalTo(accountName));
        assertThat(account.server.host(), is(host));
        assertThat(account.server.port(), is(port));
        assertThat(account.authToken, is(authToken));
        assertThat(account.room, is(room));
        assertThat(account.defaults.format, is(defaultFormat));
        assertThat(account.defaults.color, is(defaultColor));
        assertThat(account.defaults.notify, is(defaultNotify));
    }

    public void testSettingsNoAuthToken() throws Exception {
        Settings.Builder sb = Settings.builder();
        sb.put(IntegrationAccount.ROOM_SETTING, randomAlphaOfLength(10));
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(Logger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("hipchat account [_name] missing required [auth_token] setting"));
        }
    }

    public void testSettingsWithoutRoom() throws Exception {
        Settings.Builder sb = Settings.builder();
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, randomAlphaOfLength(50));
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(Logger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("missing required [room] setting for [integration] account profile"));
        }
    }

    public void testSettingsWithoutMultipleRooms() throws Exception {
        Settings.Builder sb = Settings.builder();
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, randomAlphaOfLength(50));
        sb.put(IntegrationAccount.ROOM_SETTING, "_r1,_r2");
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(Logger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("[room] setting for [integration] account must only be set with a single value"));
        }
    }

    public void testSend() throws Exception {
        String token = randomAlphaOfLength(10);
        HttpClient httpClient = mock(HttpClient.class);
        String room = "Room with Spaces";
        IntegrationAccount account = new IntegrationAccount("_name", Settings.builder()
                .put("host", "_host")
                .put("port", "443")
                .put("auth_token", token)
                .put("room", room)
                .build(), HipChatServer.DEFAULT, httpClient, mock(Logger.class));

        HipChatMessage.Format format = randomFrom(HipChatMessage.Format.values());
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        Boolean notify = randomBoolean();
        final HipChatMessage message = new HipChatMessage("_body", null, null, null, format, color, notify);

        HttpRequest req = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                // url encoded already
                .path("/v2/room/Room+with+Spaces/notification")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer " + token)
                .body(Strings.toString((builder, params) -> {
                    builder.field("message", message.body);
                    if (message.format != null) {
                        builder.field("message_format", message.format.value());
                    }
                    if (message.notify != null) {
                        builder.field("notify", message.notify);
                    }
                    if (message.color != null) {
                        builder.field("color", String.valueOf(message.color.value()));
                    }
                    return builder;
                }))
                .build();

        HttpResponse res = mock(HttpResponse.class);
        when(res.status()).thenReturn(200);
        when(httpClient.execute(req)).thenReturn(res);

        SentMessages sentMessages = account.send(message, null);
        verify(httpClient).execute(req);
        assertThat(sentMessages.asList(), hasSize(1));
        try (XContentBuilder builder = jsonBuilder()) {
            sentMessages.asList().get(0).toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), not(containsString(token)));
        }
    }
}
