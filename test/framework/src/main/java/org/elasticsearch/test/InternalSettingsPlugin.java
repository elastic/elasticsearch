/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.RemoteConnectionStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class InternalSettingsPlugin extends Plugin {

    public static final Setting<String> PROVIDED_NAME_SETTING = Setting.simpleString(
        "index.provided_name",
        Property.IndexScope,
        Property.NodeScope
    );
    public static final Setting<Boolean> MERGE_ENABLED = Setting.boolSetting(
        "index.merge.enabled",
        true,
        Property.IndexScope,
        Property.NodeScope
    );
    public static final Setting<Long> INDEX_CREATION_DATE_SETTING = Setting.longSetting(
        IndexMetadata.SETTING_CREATION_DATE,
        -1,
        -1,
        Property.IndexScope,
        Property.NodeScope
    );
    public static final Setting<TimeValue> TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.retention.check_interval",
        new TimeValue(10, TimeUnit.MINUTES),
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            MERGE_ENABLED,
            INDEX_CREATION_DATE_SETTING,
            PROVIDED_NAME_SETTING,
            TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING,
            RemoteConnectionStrategy.REMOTE_MAX_PENDING_CONNECTION_LISTENERS,
            IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING,
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING,
            IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING,
            IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING,
            FsService.ALWAYS_REFRESH_SETTING
        );
    }
}
