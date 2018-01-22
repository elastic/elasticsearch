/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Class encapsulating settings related to Index Lifecycle Management X-Pack Plugin
 */
public class LifecycleSettings {
    public static final String LIFECYCLE_POLL_INTERVAL = "indices.lifecycle.poll_interval";
    public static final String LIFECYCLE_NAME = "index.lifecycle.name";
    public static final String LIFECYCLE_PHASE = "index.lifecycle.phase";
    public static final String LIFECYCLE_ACTION = "index.lifecycle.action";
    public static final String LIFECYCLE_INDEX_CREATION_DATE = "index.lifecycle.date";

    // NORELEASE: we should probably change the default to something other than three seconds for initial release
    public static final Setting<TimeValue> LIFECYCLE_POLL_INTERVAL_SETTING = Setting.positiveTimeSetting(LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueSeconds(3), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> LIFECYCLE_NAME_SETTING = Setting.simpleString(LIFECYCLE_NAME,
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> LIFECYCLE_PHASE_SETTING = Setting.simpleString(LIFECYCLE_PHASE,
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> LIFECYCLE_ACTION_SETTING = Setting.simpleString(LIFECYCLE_ACTION,
            Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<Long> LIFECYCLE_INDEX_CREATION_DATE_SETTING = Setting.longSetting(LIFECYCLE_INDEX_CREATION_DATE,
        -1L, -1L, Setting.Property.Dynamic, Setting.Property.IndexScope);
}
