/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

/**
 * A repository that wraps a {@link BlobStoreRepository} to add settings to the index metadata during a restore to identify the source
 * snapshot and index in order to create a {@link SearchableSnapshotDirectory} (and corresponding empty translog) to search these shards
 * without needing to fully restore them.
 */
public class SearchableSnapshotRepository {

    public static final Setting<String> SNAPSHOT_REPOSITORY_SETTING =
        Setting.simpleString("index.store.snapshot.repository_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_SNAPSHOT_NAME_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_SNAPSHOT_ID_SETTING =
        Setting.simpleString("index.store.snapshot.snapshot_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<String> SNAPSHOT_INDEX_ID_SETTING =
        Setting.simpleString("index.store.snapshot.index_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
    public static final Setting<Boolean> SNAPSHOT_CACHE_ENABLED_SETTING =
        Setting.boolSetting("index.store.snapshot.cache.enabled", true, Setting.Property.IndexScope);

    public static final String SNAPSHOT_DIRECTORY_FACTORY_KEY = "snapshot";
}
