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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class V1Account extends HipChatAccount {

    public static final String TYPE = "v1";

    final Defaults defaults;

    public V1Account(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient, Logger logger) {
        super(name, Profile.V1, settings, defaultServer, httpClient, logger);
        defaults = new Defaults(settings);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void validateParsedTemplate(String watchId, String actionId,
                                       HipChatMessage.Template template) throws ElasticsearchParseException {
        if (template.users != null) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "]. ["
                    + name + "] hipchat account doesn't support user private messaging");
        }
        if ((template.rooms == null || template.rooms.length == 0) && (defaults.rooms == null || defaults.rooms.length == 0)) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "]. " +
                    "missing required [" + HipChatMessage.Field.ROOM + "] field for [" + name + "] hipchat account");
        }
    }

    @Override
    public HipChatMessage render(String watchId, String actionId, TextTemplateEngine engine, HipChatMessage.Template template,
                                 Map<String, Object> model) {
        String message = engine.render(template.body, model);
        String[] rooms = defaults.rooms;
        if (template.rooms != null) {
            rooms = new String[template.rooms.length];
            for (int i = 0; i < template.rooms.length; i++) {
                rooms[i] = engine.render(template.rooms[i], model);
            }
        }
        String from = template.from != null ? template.from : defaults.from != null ? defaults.from : watchId;
        Color color = Color.resolve(engine.render(template.color, model), defaults.color);
        Boolean notify = template.notify != null ? template.notify : defaults.notify;
        Format messageFormat = template.format != null ? template.format : defaults.format;
        return new HipChatMessage(message, rooms, null, from, messageFormat, color, notify);
    }

    @Override
    public SentMessages send(HipChatMessage message, @Nullable HttpProxy proxy) {
        List<SentMessages.SentMessage> sentMessages = new ArrayList<>();
        if (message.rooms != null) {
            for (String room : message.rooms) {
                HttpRequest request = buildRoomRequest(room, message, proxy);
                try {
                    HttpResponse response = httpClient.execute(request);
                    sentMessages.add(SentMessages.SentMessage.responded(room, SentMessages.SentMessage.TargetType.ROOM, message, request,
                            response));
                } catch (Exception e) {
                    logger.error("failed to execute hipchat api http request", e);
                    sentMessages.add(SentMessages.SentMessage.error(room, SentMessages.SentMessage.TargetType.ROOM, message, e));
                }
            }
        }
        return new SentMessages(name, sentMessages);
    }

    public HttpRequest buildRoomRequest(String room, HipChatMessage message, HttpProxy proxy) {
        HttpRequest.Builder builder = server.httpRequest();
        builder.method(HttpMethod.POST);
        builder.scheme(Scheme.HTTPS);
        builder.path("/v1/rooms/message");
        builder.setHeader("Content-Type", "application/x-www-form-urlencoded");
        builder.setParam("format", "json");
        builder.setParam("auth_token", authToken);
        if (proxy != null) {
            builder.proxy(proxy);
        }
        StringBuilder body = new StringBuilder();
        body.append("room_id=").append(HttpRequest.encodeUrl(room));
        body.append("&from=").append(HttpRequest.encodeUrl(message.from));
        body.append("&message=").append(HttpRequest.encodeUrl(message.body));
        if (message.format != null) {
            body.append("&message_format=").append(message.format.value());
        }
        if (message.color != null) {
            body.append("&color=").append(message.color.value());
        }
        if (message.notify != null) {
            body.append("&notify=").append(message.notify ? "1" : "0");
        }
        builder.body(body.toString());
        return builder.build();
    }

    static class Defaults {

        @Nullable final String[] rooms;
        @Nullable final String from;
        @Nullable final Format format;
        @Nullable final Color color;
        @Nullable final Boolean notify;

        Defaults(Settings settings) {
            List<String> rooms = settings.getAsList(DEFAULT_ROOM_SETTING, null);
            this.rooms = rooms == null ? null : rooms.toArray(Strings.EMPTY_ARRAY);
            this.from = settings.get(DEFAULT_FROM_SETTING);
            this.format = Format.resolve(settings, DEFAULT_FORMAT_SETTING, null);
            this.color = Color.resolve(settings, DEFAULT_COLOR_SETTING, null);
            this.notify = settings.getAsBoolean(DEFAULT_NOTIFY_SETTING, null);
        }
    }
}
