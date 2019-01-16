/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class CachingUsernamePasswordRealmSettings {

    private static final String CACHE_HASH_ALGO_SUFFIX = "cache.hash_algo";
    public static final Function<String, Setting.AffixSetting<String>> CACHE_HASH_ALGO_SETTING = RealmSettings.affixSetting(
            CACHE_HASH_ALGO_SUFFIX, key -> Setting.simpleString(key, "ssha256", Setting.Property.NodeScope));

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    private static final String CACHE_TTL_SUFFIX = "cache.ttl";
    public static final Function<String, Setting.AffixSetting<TimeValue>> CACHE_TTL_SETTING = RealmSettings.affixSetting(
            CACHE_TTL_SUFFIX, key -> Setting.timeSetting(key, DEFAULT_TTL, Setting.Property.NodeScope));

    private static final int DEFAULT_MAX_USERS = 100_000; //100k users
    private static final String CACHE_MAX_USERS_SUFFIX = "cache.max_users";
    public static final Function<String, Setting.AffixSetting<Integer>> CACHE_MAX_USERS_SETTING = RealmSettings.affixSetting(
            CACHE_MAX_USERS_SUFFIX, key -> Setting.intSetting(key, DEFAULT_MAX_USERS, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<Boolean>> AUTHC_ENABLED_SETTING = RealmSettings.affixSetting(
        "authentication.enabled", key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));

    private CachingUsernamePasswordRealmSettings() {
    }

    /**
     * Returns the {@link Setting setting configuration} that is common for all caching realms
     */
    public static Set<Setting.AffixSetting<?>> getSettings(String type) {
        return new HashSet<>(Arrays.asList(
                CACHE_HASH_ALGO_SETTING.apply(type),
                CACHE_TTL_SETTING.apply(type),
                CACHE_MAX_USERS_SETTING.apply(type),
                AUTHC_ENABLED_SETTING.apply(type)
        ));
    }
}
