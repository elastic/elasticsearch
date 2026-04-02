/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Cluster settings for ESQL external source caching.
 * Cache size and TTL values are restart-only (NodeScope). The enabled flag is dynamic.
 */
public final class ExternalSourceCacheSettings {

    private ExternalSourceCacheSettings() {}

    public static final Setting<ByteSizeValue> CACHE_SIZE = Setting.memorySizeSetting(
        "esql.source.cache.size",
        "0.4%",
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> CACHE_ENABLED = Setting.boolSetting(
        "esql.source.cache.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SCHEMA_TTL = Setting.positiveTimeSetting(
        "esql.source.cache.schema.ttl",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> LISTING_TTL = Setting.positiveTimeSetting(
        "esql.source.cache.listing.ttl",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(CACHE_SIZE, CACHE_ENABLED, SCHEMA_TTL, LISTING_TTL);
    }
}
