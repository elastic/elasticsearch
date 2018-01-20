/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage.Color;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage.Format;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserAccount extends HipChatAccount {

    public static final String TYPE = "user";

    final Defaults defaults;

    public UserAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient, Logger logger) {
        super(name, Profile.USER, settings, defaultServer, httpClient, logger);
        defaults = new Defaults(settings);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void validateParsedTemplate(String watchId, String actionId, HipChatMessage.Template template) throws SettingsException {
        if (template.from != null) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "]. ["
                    + name + "] hipchat account doesn't support custom `from` fields");
        }
    }

    @Override
    public HipChatMessage render(String watchId, String actionId, TextTemplateEngine engine, HipChatMessage.Template template,
                                 Map<String, Object> model) {
        String[] rooms = defaults.rooms;
        if (template.rooms != null) {
            rooms = new String[template.rooms.length];
            for (int i = 0; i < template.rooms.length; i++) {
                rooms[i] = engine.render(template.rooms[i], model);
            }
        }
        String[] users = defaults.users;
        if (template.users != null) {
            users = new String[template.users.length];
            for (int i = 0; i < template.users.length; i++) {
                users[i] = engine.render(template.users[i], model);
            }
        }
        String message = engine.render(template.body, model);
        Color color = Color.resolve(engine.render(template.color, model), defaults.color);
        Boolean notify = template.notify != null ? template.notify : defaults.notify;
        Format messageFormat = template.format != null ? template.format : defaults.format;
        return new HipChatMessage(message, rooms, users, null, messageFormat, color, notify);
    }

    @Override
    public SentMessages send(HipChatMessage message, HttpProxy proxy) {
        List<SentMessages.SentMessage> sentMessages = new ArrayList<>();
        if (message.rooms != null) {
            for (String room : message.rooms) {
                HttpRequest request = buildRoomRequest(room, message, proxy);
                try {
                    HttpResponse response = httpClient.execute(request);
                    sentMessages.add(SentMessages.SentMessage.responded(room, SentMessages.SentMessage.TargetType.ROOM, message, request,
                            response));
                } catch (IOException e) {
                    logger.error("failed to execute hipchat api http request", e);
                    sentMessages.add(SentMessages.SentMessage.error(room, SentMessages.SentMessage.TargetType.ROOM, message, e));
                }
            }
        }
        if (message.users != null) {
            for (String user : message.users) {
                HttpRequest request = buildUserRequest(user, message, proxy);
                try {
                    HttpResponse response = httpClient.execute(request);
                    sentMessages.add(SentMessages.SentMessage.responded(user, SentMessages.SentMessage.TargetType.USER, message, request,
                            response));
                } catch (Exception e) {
                    logger.error("failed to execute hipchat api http request", e);
                    sentMessages.add(SentMessages.SentMessage.error(user, SentMessages.SentMessage.TargetType.USER, message, e));
                }
            }
        }
        return new SentMessages(name, sentMessages);
    }

    public HttpRequest buildRoomRequest(String room, final HipChatMessage message, HttpProxy proxy) {
        String urlEncodedRoom = encodeRoom(room);
        HttpRequest.Builder builder = server.httpRequest()
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/room/" + urlEncodedRoom + "/notification")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer " + authToken)
                .body(Strings.toString((xbuilder, params) -> {
                    xbuilder.field("message", message.body);
                    if (message.format != null) {
                        xbuilder.field("message_format", message.format.value());
                    }
                    if (message.notify != null) {
                        xbuilder.field("notify", message.notify);
                    }
                    if (message.color != null) {
                        xbuilder.field("color", String.valueOf(message.color.value()));
                    }
                    return xbuilder;
                }));
        if (proxy != null) {
            builder.proxy(proxy);
        }
        return builder.build();
    }

    // this specific hipchat API does not accept application-form encoding, but requires real URL encoding
    // spaces must not be replaced with a plus, but rather with %20
    // this workaround ensures, that this happens
    private String encodeRoom(String text) {
        try {
            return new URI("//", "", "", text, null).getRawQuery();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("failed to URL encode text [" + text + "]", e);
        }

    }

    public HttpRequest buildUserRequest(String user, final HipChatMessage message, HttpProxy proxy) {
        HttpRequest.Builder builder = server.httpRequest()
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path("/v2/user/" + user + "/message")
                .setHeader("Content-Type", "application/json")
                .setHeader("Authorization", "Bearer " + authToken)
                .body(Strings.toString((xbuilder, params) -> {
                    xbuilder.field("message", message.body);
                    if (message.format != null) {
                        xbuilder.field("message_format", message.format.value());
                    }
                    if (message.notify != null) {
                        xbuilder.field("notify", message.notify);
                    }
                    return xbuilder;
                }));
        if (proxy != null) {
            builder.proxy(proxy);
        }
        return builder.build();
    }

    static class Defaults {

        @Nullable final String[] rooms;
        @Nullable final String[] users;
        @Nullable final Format format;
        @Nullable final Color color;
        @Nullable final Boolean notify;

        Defaults(Settings settings) {
            List<String> rooms = settings.getAsList(DEFAULT_ROOM_SETTING, null);
            this.rooms = rooms == null ? null : rooms.toArray(Strings.EMPTY_ARRAY);
            List<String> users = settings.getAsList(DEFAULT_USER_SETTING, null);
            this.users = users == null ? null : users.toArray(Strings.EMPTY_ARRAY);
            this.format = Format.resolve(settings, DEFAULT_FORMAT_SETTING, null);
            this.color = Color.resolve(settings, DEFAULT_COLOR_SETTING, null);
            this.notify = settings.getAsBoolean(DEFAULT_NOTIFY_SETTING, null);
        }
    }
}
