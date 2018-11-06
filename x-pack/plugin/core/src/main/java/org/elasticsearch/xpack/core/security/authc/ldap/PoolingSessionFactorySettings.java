/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.SecureSetting.secureString;

public final class PoolingSessionFactorySettings {
    public static final TimeValue DEFAULT_HEALTH_CHECK_INTERVAL = TimeValue.timeValueSeconds(60L);

    public static final String BIND_DN_SUFFIX = "bind_dn";
    public static final Function<String, Setting.AffixSetting<String>> BIND_DN = RealmSettings.affixSetting(BIND_DN_SUFFIX,
            key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Filtered));

    public static final Function<String, Setting.AffixSetting<SecureString>> LEGACY_BIND_PASSWORD = RealmSettings.affixSetting(
            "bind_password", key -> new Setting<>(key, "", SecureString::new,
                    Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Deprecated));

    public static final Function<String, Setting.AffixSetting<SecureString>> SECURE_BIND_PASSWORD = realmType ->
            Setting.affixKeySetting(
                    RealmSettings.realmSettingPrefix(realmType), "secure_bind_password",
                    key -> secureString(key, null)
            );

    public static final int DEFAULT_CONNECTION_POOL_INITIAL_SIZE = 0;
    public static final Function<String, Setting.AffixSetting<Integer>> POOL_INITIAL_SIZE = RealmSettings.affixSetting(
            "user_search.pool.initial_size",
            key -> Setting.intSetting(key, DEFAULT_CONNECTION_POOL_INITIAL_SIZE, 0, Setting.Property.NodeScope));

    public static final int DEFAULT_CONNECTION_POOL_SIZE = 20;
    public static final Function<String, Setting.AffixSetting<Integer>> POOL_SIZE = RealmSettings.affixSetting("user_search.pool.size",
            key -> Setting.intSetting(key, DEFAULT_CONNECTION_POOL_SIZE, 1, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<TimeValue>> HEALTH_CHECK_INTERVAL = RealmSettings.affixSetting(
            "user_search.pool.health_check.interval",
            key -> Setting.timeSetting(key, DEFAULT_HEALTH_CHECK_INTERVAL, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<Boolean>> HEALTH_CHECK_ENABLED = RealmSettings.affixSetting(
            "user_search.pool.health_check.enabled",
            key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<Optional<String>>> HEALTH_CHECK_DN = RealmSettings.affixSetting(
            "user_search.pool.health_check.dn",
            key -> new Setting<>(key, (String) null,
                    Optional::ofNullable, Setting.Property.NodeScope));

    private PoolingSessionFactorySettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings(String realmType) {
        return Stream.of(
            POOL_INITIAL_SIZE, POOL_SIZE, HEALTH_CHECK_ENABLED, HEALTH_CHECK_INTERVAL, HEALTH_CHECK_DN, BIND_DN,
            LEGACY_BIND_PASSWORD, SECURE_BIND_PASSWORD
        ).map(f -> f.apply(realmType)).collect(Collectors.toSet());
    }
}
