/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Class encapsulating settings related to Index Lifecycle Management X-Pack Plugin
 */
public class LifecycleSettings {
    public static final String LIFECYCLE_POLL_INTERVAL = "indices.lifecycle.poll_interval";
    public static final String LIFECYCLE_NAME = "index.lifecycle.name";
    public static final String LIFECYCLE_INDEXING_COMPLETE = "index.lifecycle.indexing_complete";

    public static final String SLM_HISTORY_INDEX_ENABLED = "slm.history_index_enabled";

    public static final Setting<TimeValue> LIFECYCLE_POLL_INTERVAL_SETTING = Setting.timeSetting(LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueMinutes(10), TimeValue.timeValueSeconds(1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> LIFECYCLE_NAME_SETTING = Setting.simpleString(LIFECYCLE_NAME,
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<Boolean> LIFECYCLE_INDEXING_COMPLETE_SETTING = Setting.boolSetting(LIFECYCLE_INDEXING_COMPLETE, false,
        Setting.Property.Dynamic, Setting.Property.IndexScope);

    public static final Setting<Boolean> SLM_HISTORY_INDEX_ENABLED_SETTING = Setting.boolSetting(SLM_HISTORY_INDEX_ENABLED, true,
        Setting.Property.NodeScope);
}
