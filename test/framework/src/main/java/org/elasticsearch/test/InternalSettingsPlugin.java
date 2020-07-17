/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.RemoteConnectionStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class InternalSettingsPlugin extends Plugin {

    public static final Setting<String> PROVIDED_NAME_SETTING =
        Setting.simpleString("index.provided_name",Property.IndexScope, Property.NodeScope);
    public static final Setting<Boolean> MERGE_ENABLED =
        Setting.boolSetting("index.merge.enabled", true, Property.IndexScope, Property.NodeScope);
    public static final Setting<Long> INDEX_CREATION_DATE_SETTING =
        Setting.longSetting(IndexMetadata.SETTING_CREATION_DATE, -1, -1, Property.IndexScope, Property.NodeScope);
    public static final Setting<TimeValue> TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("index.translog.retention.check_interval", new TimeValue(10, TimeUnit.MINUTES),
            new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

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
                IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING
            );
    }
}
