/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

/**
 * Represents standard settings for the ServiceProvider cache(s) in the IdP
 */
public final class ServiceProviderCacheSettings {
    private static final int CACHE_SIZE_DEFAULT = 1000;
    private static final TimeValue CACHE_TTL_DEFAULT = TimeValue.timeValueMinutes(60);

    public static final Setting<Integer> CACHE_SIZE
        = Setting.intSetting("xpack.idp.sp.cache.size", CACHE_SIZE_DEFAULT, Setting.Property.NodeScope);
    public static final Setting<TimeValue> CACHE_TTL
        = Setting.timeSetting("xpack.idp.sp.cache.ttl", CACHE_TTL_DEFAULT, Setting.Property.NodeScope);

    static <K, V> Cache<K, V> buildCache(Settings settings) {
        return CacheBuilder.<K, V>builder()
            .setMaximumWeight(CACHE_SIZE.get(settings))
            .setExpireAfterAccess(CACHE_TTL.get(settings))
            .build();
    }

    public static List<Setting<?>> getSettings() {
        return List.of(CACHE_SIZE, CACHE_TTL);
    }
}
