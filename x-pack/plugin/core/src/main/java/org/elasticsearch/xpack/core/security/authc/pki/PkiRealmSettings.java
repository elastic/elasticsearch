/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.pki;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.CompositeRoleMapperSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public final class PkiRealmSettings {
    public static final String TYPE = "pki";
    public static final String DEFAULT_USERNAME_PATTERN = "CN=(.*?)(?:,|$)";
    public static final Setting<Pattern> USERNAME_PATTERN_SETTING = new Setting<>("username_pattern", DEFAULT_USERNAME_PATTERN,
            s -> Pattern.compile(s, Pattern.CASE_INSENSITIVE), Setting.Property.NodeScope);
    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("cache.ttl", DEFAULT_TTL, Setting.Property.NodeScope);
    private static final int DEFAULT_MAX_USERS = 100_000; //100k users
    public static final Setting<Integer> CACHE_MAX_USERS_SETTING = Setting.intSetting("cache.max_users", DEFAULT_MAX_USERS,
            Setting.Property.NodeScope);
    public static final SSLConfigurationSettings SSL_SETTINGS = SSLConfigurationSettings.withoutPrefix();

    private PkiRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.add(USERNAME_PATTERN_SETTING);
        settings.add(CACHE_TTL_SETTING);
        settings.add(CACHE_MAX_USERS_SETTING);

        settings.add(SSL_SETTINGS.truststorePath);
        settings.add(SSL_SETTINGS.truststorePassword);
        settings.add(SSL_SETTINGS.legacyTruststorePassword);
        settings.add(SSL_SETTINGS.truststoreAlgorithm);
        settings.add(SSL_SETTINGS.caPaths);

        settings.addAll(DelegatedAuthorizationSettings.getSettings());
        settings.addAll(CompositeRoleMapperSettings.getSettings());

        return settings;
    }
}
