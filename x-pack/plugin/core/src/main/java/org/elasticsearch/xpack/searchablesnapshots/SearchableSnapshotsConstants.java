/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

public class SearchableSnapshotsConstants {
    public static final String SNAPSHOT_DIRECTORY_FACTORY_KEY = "snapshot";

    public static final String SNAPSHOT_RECOVERY_STATE_FACTORY_KEY = "snapshot_prewarm";

    public static boolean isSearchableSnapshotStore(Settings indexSettings) {
        return SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings));
    }

    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_NAME = "searchable_snapshots_cache_fetch_async";
    public static final String CACHE_FETCH_ASYNC_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_fetch_async_thread_pool";

    public static final String CACHE_PREWARMING_THREAD_POOL_NAME = "searchable_snapshots_cache_prewarming";
    public static final String CACHE_PREWARMING_THREAD_POOL_SETTING = "xpack.searchable_snapshots.cache_prewarming_thread_pool";

    public static final String SNAPSHOT_BLOB_CACHE_INDEX = ".snapshot-blob-cache";

    /**
     * We use {@code long} to represent offsets and lengths of files since they may be larger than 2GB, but {@code int} to represent
     * offsets and lengths of arrays in memory which are limited to 2GB in size. We quite often need to convert from the file-based world
     * of {@code long}s into the memory-based world of {@code int}s, knowing for certain that the result will not overflow. This method
     * should be used to clarify that we're doing this.
     */
    public static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }

}
