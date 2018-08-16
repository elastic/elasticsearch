/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class CachingUsernamePasswordRealmSettings {
    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString("cache.hash_algo", "ssha256",
        Setting.Property.NodeScope);
    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("cache.ttl", DEFAULT_TTL, Setting.Property.NodeScope);
    private static final int DEFAULT_MAX_USERS = 100_000; //100k users
    public static final Setting<Integer> CACHE_MAX_USERS_SETTING = Setting.intSetting("cache.max_users", DEFAULT_MAX_USERS,
            Setting.Property.NodeScope);

    private CachingUsernamePasswordRealmSettings() {}

    /**
     * Returns the {@link Setting setting configuration} that is common for all caching realms
     */
    public static Set<Setting<?>> getCachingSettings() {
        return new HashSet<>(Arrays.asList(CACHE_HASH_ALGO_SETTING, CACHE_TTL_SETTING, CACHE_MAX_USERS_SETTING));
    }
}
