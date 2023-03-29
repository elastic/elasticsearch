/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.blobcache;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

public class BlobCachePlugin extends Plugin implements ExtensiblePlugin {

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING,
            SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING,
            SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING,
            SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING,
            SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING,
            SharedBlobCacheService.SHARED_CACHE_MAX_FREQ_SETTING,
            SharedBlobCacheService.SHARED_CACHE_DECAY_INTERVAL_SETTING,
            SharedBlobCacheService.SHARED_CACHE_MIN_TIME_DELTA_SETTING
        );
    }
}
