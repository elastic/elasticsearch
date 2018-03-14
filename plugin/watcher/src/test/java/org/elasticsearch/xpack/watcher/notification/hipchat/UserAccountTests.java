/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserAccountTests extends ESTestCase {

    public void testSettings() throws Exception {
        String accountName = "_name";

        Settings.Builder sb = Settings.builder();

        String authToken = randomAlphaOfLength(50);
        sb.put(UserAccount.AUTH_TOKEN_SETTING, authToken);

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

        String[] defaultRooms = null;
        if (randomBoolean()) {
            defaultRooms = new String[] { "_r1", "_r2" };
            sb.put(HipChatAccount.DEFAULT_ROOM_SETTING, "_r1,_r2");
        }
        String[] defaultUsers = null;
        if (randomBoolean()) {
            defaultUsers = new String[] { "_u1", "_u2" };
            sb.put(HipChatAccount.DEFAULT_USER_SETTING, "_u1,_u2");
        }
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

        UserAccount account = new UserAccount(accountName, settings, HipChatServer.DEFAULT, mock(HttpClient.class), mock(Logger.class));

        assertThat(account.profile, is(HipChatAccount.Profile.USER));
        assertThat(account.name, equalTo(accountName));
        assertThat(account.server.host(), is(host));
        assertThat(account.server.port(), is(port));
        assertThat(account.authToken, is(authToken));
        if (defaultRooms != null) {
            assertThat(account.defaults.rooms, arrayContaining(defaultRooms));
        } else {
            assertThat(account.defaults.rooms, nullValue());
        }
        if (defaultUsers != null) {
            assertThat(account.defaults.users, arrayContaining(defaultUsers));
        } else {
            assertThat(account.defaults.users, nullValue());
        }
        assertThat(account.defaults.format, is(defaultFormat));
        assertThat(account.defaults.color, is(defaultColor));
        assertThat(account.defaults.notify, is(defaultNotify));
    }

    public void testSettingsNoAuthToken() throws Exception {
        Settings.Builder sb = Settings.builder();
        try {
            new UserAccount("_name", sb.build(), HipChatServer.DEFAULT, mock(HttpClient.class), mock(Logger.class));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("hipchat account [_name] missing required [auth_token] setting"));
        }
    }

    public void testSend() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        UserAccount account = new UserAccount("_name", Settings.builder()
                .put("host", "_host")
                .put("port", "443")
                .put("auth_token", "_token")
                .build(), HipChatServer.DEFAULT, httpClient, mock(Logger.class));

        HipChatMessage.Format format = randomFrom(HipChatMessage.Format.values());
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        Boolean notify = randomBoolean();
        final HipChatMessage message = new HipChatMessage("_body", new String[] { "_r1", "_r2" }, new String[] { "_u1", "_u2" }, null,
                format, color, notify);

        HttpRequest reqR1 = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/room/_r1/notification")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer _token")
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

        logger.info("expected (r1): {}", BytesReference.bytes(jsonBuilder().value(reqR1)).utf8ToString());

        HttpResponse resR1 = mock(HttpResponse.class);
        when(resR1.status()).thenReturn(200);
        when(httpClient.execute(reqR1)).thenReturn(resR1);

        HttpRequest reqR2 = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/room/_r2/notification")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer _token")
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

        logger.info("expected (r2): {}", BytesReference.bytes(jsonBuilder().value(reqR1)).utf8ToString());

        HttpResponse resR2 = mock(HttpResponse.class);
        when(resR2.status()).thenReturn(200);
        when(httpClient.execute(reqR2)).thenReturn(resR2);

        HttpRequest reqU1 = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/user/_u1/message")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer _token")
                .body(Strings.toString((builder, params) -> {
                    builder.field("message", message.body);
                    if (message.format != null) {
                        builder.field("message_format", message.format.value());
                    }
                    if (message.notify != null) {
                        builder.field("notify", message.notify);
                    }
                    return builder;
                }))
                .build();

        logger.info("expected (u1): {}", BytesReference.bytes(jsonBuilder().value(reqU1)).utf8ToString());

        HttpResponse resU1 = mock(HttpResponse.class);
        when(resU1.status()).thenReturn(200);
        when(httpClient.execute(reqU1)).thenReturn(resU1);

        HttpRequest reqU2 = HttpRequest.builder("_host", 443)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/user/_u2/message")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer _token")
                .body(Strings.toString((builder, params) -> {
                    builder.field("message", message.body);
                    if (message.format != null) {
                        builder.field("message_format", message.format.value());
                    }
                    if (message.notify != null) {
                        builder.field("notify", message.notify);
                    }
                    return builder;
                }))
                .build();

        logger.info("expected (u2): {}", BytesReference.bytes(jsonBuilder().value(reqU2)).utf8ToString());

        HttpResponse resU2 = mock(HttpResponse.class);
        when(resU2.status()).thenReturn(200);
        when(httpClient.execute(reqU2)).thenReturn(resU2);

        account.send(message, null);

        verify(httpClient).execute(reqR1);
        verify(httpClient).execute(reqR2);
        verify(httpClient).execute(reqU2);
        verify(httpClient).execute(reqU2);
    }

    public void testColorIsOptional() throws Exception {
        Settings settings = Settings.builder()
                .put("user", "testuser")
                .put("auth_token", "awesome-auth-token")
                .build();
        UserAccount userAccount = createUserAccount(settings);

        TextTemplate body = new TextTemplate("body");
        TextTemplate[] rooms = new TextTemplate[] { new TextTemplate("room")};
        HipChatMessage.Template template =
                new HipChatMessage.Template(body, rooms, null, "sender", HipChatMessage.Format.TEXT, null, true);

        HipChatMessage message = userAccount.render("watchId", "actionId", new MockTextTemplateEngine(), template, new HashMap<>());
        assertThat(message.color, is(nullValue()));
    }

    public void testFormatIsOptional() throws Exception {
        Settings settings = Settings.builder()
                .put("user", "testuser")
                .put("auth_token", "awesome-auth-token")
                .build();
        UserAccount userAccount = createUserAccount(settings);

        TextTemplate body = new TextTemplate("body");
        TextTemplate[] rooms = new TextTemplate[] { new TextTemplate("room") };
        HipChatMessage.Template template = new HipChatMessage.Template(body, rooms, null, "sender", null,
                new TextTemplate("yellow"), true);

        HipChatMessage message = userAccount.render("watchId", "actionId", new MockTextTemplateEngine(), template, new HashMap<>());
        assertThat(message.format, is(nullValue()));
    }

    public void testRoomNameIsUrlEncoded() throws Exception {
        Settings settings = Settings.builder()
                .put("user", "testuser")
                .put("auth_token", "awesome-auth-token")
                .build();
        HipChatServer hipChatServer = mock(HipChatServer.class);
        HttpClient httpClient = mock(HttpClient.class);
        UserAccount account = new UserAccount("notify-monitoring", settings, hipChatServer, httpClient, logger);

        TextTemplate[] rooms = new TextTemplate[] { new TextTemplate("Room with Spaces")};
        HipChatMessage.Template template =
                new HipChatMessage.Template(new TextTemplate("body"), rooms, null, "sender", HipChatMessage.Format.TEXT, null, true);

        HipChatMessage message = account.render("watchId", "actionId", new MockTextTemplateEngine(), template, new HashMap<>());
        account.send(message, HttpProxy.NO_PROXY);

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).execute(captor.capture());
        assertThat(captor.getAllValues(), hasSize(1));
        assertThat(captor.getValue().path(), not(containsString("Room with Spaces")));
        assertThat(captor.getValue().path(), containsString("Room%20with%20Spaces"));
    }

    private UserAccount createUserAccount(Settings settings) {
        HipChatServer hipChatServer = mock(HipChatServer.class);
        HttpClient httpClient = mock(HttpClient.class);
        return new UserAccount("notify-monitoring", settings, hipChatServer, httpClient, logger);
    }
}
