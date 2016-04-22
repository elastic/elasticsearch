/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpMethod;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.http.Scheme;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IntegrationAccountTests extends ESTestCase {
    public void testSettings() throws Exception {
        String accountName = "_name";

        Settings.Builder sb = Settings.builder();

        String authToken = randomAsciiOfLength(50);
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, authToken);

        String host = HipChatServer.DEFAULT.host();
        if (randomBoolean()) {
            host = randomAsciiOfLength(10);
            sb.put(HipChatServer.HOST_SETTING, host);
        }
        int port = HipChatServer.DEFAULT.port();
        if (randomBoolean()) {
            port = randomIntBetween(300, 400);
            sb.put(HipChatServer.PORT_SETTING, port);
        }

        String room = randomAsciiOfLength(10);
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
                mock(ESLogger.class));

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
        sb.put(IntegrationAccount.ROOM_SETTING, randomAsciiOfLength(10));
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(ESLogger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("hipchat account [_name] missing required [auth_token] setting"));
        }
    }

    public void testSettingsWithoutRoom() throws Exception {
        Settings.Builder sb = Settings.builder();
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, randomAsciiOfLength(50));
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(ESLogger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("missing required [room] setting for [integration] account profile"));
        }
    }

    public void testSettingsWithoutMultipleRooms() throws Exception {
        Settings.Builder sb = Settings.builder();
        sb.put(IntegrationAccount.AUTH_TOKEN_SETTING, randomAsciiOfLength(50));
        sb.put(IntegrationAccount.ROOM_SETTING, "_r1,_r2");
        try {
            new IntegrationAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(ESLogger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), containsString("[room] setting for [integration] account must only be set with a single value"));
        }
    }

    public void testSend() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        IntegrationAccount account = new IntegrationAccount("_name", Settings.builder()
                .put("host", "_host")
                .put("port", "443")
                .put("auth_token", "_token")
                .put("room", "_room")
                .build(), HipChatServer.DEFAULT, httpClient, mock(ESLogger.class));

        HipChatMessage.Format format = randomFrom(HipChatMessage.Format.values());
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        Boolean notify = randomBoolean();
        final HipChatMessage message = new HipChatMessage("_body", null, null, null, format, color, notify);

        HttpRequest req = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/room/_room/notification")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer _token")
                .body(XContentHelper.toString(new ToXContent() {
                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
                    }
                }))
                .build();

        HttpResponse res = mock(HttpResponse.class);
        when(res.status()).thenReturn(200);
        when(httpClient.execute(req)).thenReturn(res);

        account.send(message);

        verify(httpClient).execute(req);
    }
}
