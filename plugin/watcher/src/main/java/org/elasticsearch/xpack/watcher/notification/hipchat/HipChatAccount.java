/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public abstract class HipChatAccount  {

    public static final String AUTH_TOKEN_SETTING = "auth_token";
    public static final String ROOM_SETTING = HipChatMessage.Field.ROOM.getPreferredName();
    public static final String DEFAULT_ROOM_SETTING = "message_defaults." + HipChatMessage.Field.ROOM.getPreferredName();
    public static final String DEFAULT_USER_SETTING = "message_defaults." + HipChatMessage.Field.USER.getPreferredName();
    public static final String DEFAULT_FROM_SETTING = "message_defaults." + HipChatMessage.Field.FROM.getPreferredName();
    public static final String DEFAULT_FORMAT_SETTING = "message_defaults." + HipChatMessage.Field.FORMAT.getPreferredName();
    public static final String DEFAULT_COLOR_SETTING = "message_defaults." + HipChatMessage.Field.COLOR.getPreferredName();
    public static final String DEFAULT_NOTIFY_SETTING = "message_defaults." + HipChatMessage.Field.NOTIFY.getPreferredName();

    private static final Setting<SecureString> SECURE_AUTH_TOKEN_SETTING = SecureSetting.secureString("secure_" + AUTH_TOKEN_SETTING, null);

    protected final Logger logger;
    protected final String name;
    protected final Profile profile;
    protected final HipChatServer server;
    protected final HttpClient httpClient;
    protected final String authToken;

    protected HipChatAccount(String name, Profile profile, Settings settings, HipChatServer defaultServer, HttpClient httpClient,
                             Logger logger) {
        this.name = name;
        this.profile = profile;
        this.server = new HipChatServer(settings, defaultServer);
        this.httpClient = httpClient;
        this.authToken = getAuthToken(name, settings);
        this.logger = logger;
    }

    private static String getAuthToken(String name, Settings settings) {
        String authToken = settings.get(AUTH_TOKEN_SETTING);
        if (authToken == null || authToken.length() == 0) {
            SecureString secureString = SECURE_AUTH_TOKEN_SETTING.get(settings);
            if (secureString == null || secureString.length() < 1) {
                throw new SettingsException("hipchat account [" + name + "] missing required [" + AUTH_TOKEN_SETTING + "] setting");
            }
            authToken = secureString.toString();
        }

        return authToken;
    }

    public abstract String type();

    public abstract void validateParsedTemplate(String watchId, String actionId, HipChatMessage.Template message) throws SettingsException;

    public abstract HipChatMessage render(String watchId, String actionId, TextTemplateEngine engine, HipChatMessage.Template template,
                                          Map<String, Object> model);

    public abstract SentMessages send(HipChatMessage message, @Nullable HttpProxy proxy);

    public enum Profile {

        V1() {
            @Override
            HipChatAccount createAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient,
                                         Logger logger) {
                return new V1Account(name, settings, defaultServer, httpClient, logger);
            }
        },
        INTEGRATION() {
            @Override
            HipChatAccount createAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient,
                                         Logger logger) {
                return new IntegrationAccount(name, settings, defaultServer, httpClient, logger);
            }
        },
        USER() {
            @Override
            HipChatAccount createAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient,
                                         Logger logger) {
                return new UserAccount(name, settings, defaultServer, httpClient, logger);
            }
        };

        abstract HipChatAccount createAccount(String name, Settings settings, HipChatServer defaultServer, HttpClient httpClient,
                                              Logger logger);

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Profile parse(XContentParser parser) throws IOException {
            return Profile.valueOf(parser.text().toUpperCase(Locale.ROOT));
        }

        public static Profile resolve(String value, Profile defaultValue) {
            if (value == null) {
                return defaultValue;
            }
            return Profile.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Profile resolve(Settings settings, String setting, Profile defaultValue) {
            return resolve(settings.get(setting), defaultValue);
        }

        public static boolean validate(String value) {
            try {
                Profile.valueOf(value.toUpperCase(Locale.ROOT));
                return true;
            } catch (IllegalArgumentException ilae) {
                return false;
            }
        }
    }
}
