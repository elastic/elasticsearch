/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;

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

    public static final Version SHARED_CACHE_VERSION = Version.V_8_0_0;

}
