/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Class encapsulating settings related to Index Lifecycle Management X-Pack Plugin
 */
public class LifecycleSettings {
    public static final String LIFECYCLE_POLL_INTERVAL = "indices.lifecycle.poll_interval";
    public static final String LIFECYCLE_MAINTENANCE_MODE = "indices.lifecycle.maintenance";
    public static final String LIFECYCLE_NAME = "index.lifecycle.name";
    public static final String LIFECYCLE_PHASE = "index.lifecycle.phase";
    public static final String LIFECYCLE_ACTION = "index.lifecycle.action";
    public static final String LIFECYCLE_STEP = "index.lifecycle.step";
    public static final String LIFECYCLE_INDEX_CREATION_DATE = "index.lifecycle.date";
    public static final String LIFECYCLE_PHASE_TIME = "index.lifecycle.phase_time";
    public static final String LIFECYCLE_ACTION_TIME = "index.lifecycle.action_time";
    public static final String LIFECYCLE_STEP_TIME = "index.lifecycle.step_time";
    public static final String LIFECYCLE_FAILED_STEP = "index.lifecycle.failed_step";
    public static final String LIFECYCLE_STEP_INFO = "index.lifecycle.step_info";
    public static final String LIFECYCLE_SKIP = "index.lifecycle.skip";

    // NORELEASE: we should probably change the default to something other than three seconds for initial release
    public static final Setting<TimeValue> LIFECYCLE_POLL_INTERVAL_SETTING = Setting.positiveTimeSetting(LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueSeconds(3), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> LIFECYCLE_NAME_SETTING = Setting.simpleString(LIFECYCLE_NAME,
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> LIFECYCLE_PHASE_SETTING = Setting.simpleString(LIFECYCLE_PHASE,
        Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<String> LIFECYCLE_ACTION_SETTING = Setting.simpleString(LIFECYCLE_ACTION,
            Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<String> LIFECYCLE_STEP_SETTING = Setting.simpleString(LIFECYCLE_STEP,
        Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<String> LIFECYCLE_FAILED_STEP_SETTING = Setting.simpleString(LIFECYCLE_FAILED_STEP,
            Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<Long> LIFECYCLE_INDEX_CREATION_DATE_SETTING = Setting.longSetting(LIFECYCLE_INDEX_CREATION_DATE,
        -1L, -1L, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<Long> LIFECYCLE_PHASE_TIME_SETTING = Setting.longSetting(LIFECYCLE_PHASE_TIME,
        -1L, -1L, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<Long> LIFECYCLE_ACTION_TIME_SETTING = Setting.longSetting(LIFECYCLE_ACTION_TIME,
        -1L, -1L, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<Long> LIFECYCLE_STEP_TIME_SETTING = Setting.longSetting(LIFECYCLE_STEP_TIME,
        -1L, -1L, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.InternalIndex);
    public static final Setting<String> LIFECYCLE_STEP_INFO_SETTING = Setting.simpleString(LIFECYCLE_STEP_INFO, Setting.Property.Dynamic,
            Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize, Setting.Property.InternalIndex);
    public static final Setting<Boolean> LIFECYCLE_SKIP_SETTING = Setting.boolSetting(LIFECYCLE_SKIP, false,
        Setting.Property.Dynamic, Setting.Property.IndexScope);
}
