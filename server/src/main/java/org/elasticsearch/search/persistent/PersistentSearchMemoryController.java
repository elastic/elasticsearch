/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

public class PersistentSearchMemoryController {

    /**
     * How much heap (% or bytes) it will be used to store intermediate search results in heap (5% by default).
     */
    public static final Setting<ByteSizeValue> CACHE_MAX_SIZE_SETTING =
        Setting.memorySizeSetting("persistent_search.memory.cache_buffer_size", "5%", Setting.Property.NodeScope);

    private final ByteSizeValue cacheMaxSize;

    public PersistentSearchMemoryController(Settings settings) {
        this.cacheMaxSize = CACHE_MAX_SIZE_SETTING.get(settings);
    }

    public void trackSearchResultSize(long size) {

    }

    public void removedFromCache(long size) {

    }
}
