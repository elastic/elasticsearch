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
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage.Color;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage.Format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IntegrationAccount extends HipChatAccount {

    public static final String TYPE = "integration";

    final String room;
    final Defaults defaults;

    public IntegrationAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient, Logger logger) {
        super(name, Profile.INTEGRATION, settings, defaultServer, httpClient, logger);
        List<String> rooms = settings.getAsList(ROOM_SETTING, null);
        if (rooms == null || rooms.isEmpty()) {
            throw new SettingsException("invalid hipchat account [" + name + "]. missing required [" + ROOM_SETTING + "] setting for [" +
                    TYPE + "] account profile");
        }
        if (rooms.size() > 1) {
            throw new SettingsException("invalid hipchat account [" + name + "]. [" + ROOM_SETTING + "] setting for [" + TYPE + "] " +
                    "account must only be set with a single value");
        }
        this.room = rooms.get(0);
        defaults = new Defaults(settings);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void validateParsedTemplate(String watchId, String actionId, HipChatMessage.Template template) throws SettingsException {
        if (template.rooms != null) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "] " +
                    "action. [" + name + "] hipchat account doesn't support custom rooms");
        }
        if (template.users != null) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "] " +
                    "action. [" + name + "] hipchat account doesn't support user private messages");
        }
        if (template.from != null) {
            throw new ElasticsearchParseException("invalid [" + HipChatAction.TYPE + "] action for [" + watchId + "/" + actionId + "] " +
                    "action. [" + name + "] hipchat account doesn't support custom `from` fields");
        }
    }

    @Override
    public HipChatMessage render(String watchId, String actionId, TextTemplateEngine engine, HipChatMessage.Template template,
                                 Map<String, Object> model) {
        String message = engine.render(template.body, model);
        Color color = template.color != null ? Color.resolve(engine.render(template.color, model), defaults.color) : defaults.color;
        Boolean notify = template.notify != null ? template.notify : defaults.notify;
        Format messageFormat = template.format != null ? template.format : defaults.format;
        return new HipChatMessage(message, null, null, null, messageFormat, color, notify);
    }

    @Override
    public SentMessages send(HipChatMessage message, @Nullable HttpProxy proxy) {
        List<SentMessages.SentMessage> sentMessages = new ArrayList<>();
        HttpRequest request = buildRoomRequest(room, message, proxy);
        try {
            HttpResponse response = httpClient.execute(request);
            sentMessages.add(SentMessages.SentMessage.responded(room, SentMessages.SentMessage.TargetType.ROOM, message, request,
                    response));
        } catch (Exception e) {
            sentMessages.add(SentMessages.SentMessage.error(room, SentMessages.SentMessage.TargetType.ROOM, message, e));
        }
        return new SentMessages(name, sentMessages);
    }

    private HttpRequest buildRoomRequest(String room, final HipChatMessage message, HttpProxy proxy) {
        String urlEncodedRoom = HttpRequest.encodeUrl(room);
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

    static class Defaults {

        @Nullable final Format format;
        @Nullable final Color color;
        @Nullable final Boolean notify;

        Defaults(Settings settings) {
            this.format = Format.resolve(settings, DEFAULT_FORMAT_SETTING, null);
            this.color = Color.resolve(settings, DEFAULT_COLOR_SETTING, null);
            this.notify = settings.getAsBoolean(DEFAULT_NOTIFY_SETTING, null);
        }
    }
}
