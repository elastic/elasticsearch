/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.nio.file.Path;

/**
 * {@link CacheService} maintains a cache entry for every file read from cached searchable snapshot directories (see {@link CacheDirectory})
 */
public class CacheService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(CacheService.class);

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting("searchable.snapshot.cache.size",
        new ByteSizeValue(1, ByteSizeUnit.GB),                  // default
        new ByteSizeValue(1, ByteSizeUnit.KB),                  // min
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // max
        Setting.Property.NodeScope);

    private final Cache<Path, Object> cache;

    public CacheService(final Settings settings) {
        this.cache = CacheBuilder.<Path, Object>builder()
            .setMaximumWeight(SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes())
            .build();
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        cache.invalidateAll();
    }

    @Override
    protected void doClose() throws IOException {
    }

}
