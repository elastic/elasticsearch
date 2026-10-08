/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.Map;

/**
 * This class holds dynamic settings to control the behavior of {@link SearchCommitPrefetcher}.
 * <p>
 * It listens to changes in the cluster settings and updates its internal state accordingly.
 */
public class SearchCommitPrefetcherDynamicSettings {

    private static final TimeValue DEFAULT_SEARCH_IDLE_TIME = TimeValue.timeValueDays(3);
    public static final Setting<Boolean> PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> PREFETCH_SEARCH_IDLE_TIME_SETTING = Setting.timeSetting(
        "stateless.search.prefetch_commits.search_idle_time",
        DEFAULT_SEARCH_IDLE_TIME,
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    // this is not really a prefetch-related setting, however it is only checked during prefetching, when processing commit notifications
    // (we can't have the cluster settings updater hold references to each eg {@link SearchEngine} because that's a memory leak)
    public static final Setting<Boolean> STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT = Setting.boolSetting(
        "stateless.search.use_internal_files_replicated_content",
        true,
        // Cache boost preference requires per-CC timestamps in {@link BlobFileRanges} which are only built up when this setting is enabled.
        // The validator here mirrors the one on STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING so that a dynamic flip of this setting is
        // rejected if it would leave boost in an invalid state.
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                final boolean boostEnabled = (boolean) settings.get(
                    StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING
                );
                if (value == false && boostEnabled) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Setting [%s] cannot be [false] while setting [%s] is [true]",
                            STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(),
                            StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey()
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING);
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private volatile boolean prefetchingEnabled;
    private volatile long prefetchSearchIdleTimeInMillis;
    private volatile boolean useInternalFilesReplicatedContentForSearchShards;

    public SearchCommitPrefetcherDynamicSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING, value -> this.prefetchingEnabled = value);
        clusterSettings.initializeAndWatch(
            PREFETCH_SEARCH_IDLE_TIME_SETTING,
            value -> this.prefetchSearchIdleTimeInMillis = value.millis()
        );
        clusterSettings.initializeAndWatch(
            STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT,
            value -> this.useInternalFilesReplicatedContentForSearchShards = value
        );
    }

    public boolean prefetchingEnabled() {
        return prefetchingEnabled;
    }

    public long searchIdleTimeInMillis() {
        return prefetchSearchIdleTimeInMillis;
    }

    public boolean internalFilesReplicatedContentForSearchShardsEnabled() {
        return useInternalFilesReplicatedContentForSearchShards;
    }
}
