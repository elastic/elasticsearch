/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;

/**
 * Class encapsulating settings related to Index Lifecycle Management X-Pack Plugin
 */
public class LifecycleSettings {
    public static final String LIFECYCLE_POLL_INTERVAL = "indices.lifecycle.poll_interval";
    public static final String LIFECYCLE_NAME = IndexMetadata.LIFECYCLE_NAME;
    public static final String LIFECYCLE_INDEXING_COMPLETE = "index.lifecycle.indexing_complete";
    public static final String LIFECYCLE_ORIGINATION_DATE = "index.lifecycle.origination_date";
    public static final String LIFECYCLE_PARSE_ORIGINATION_DATE = "index.lifecycle.parse_origination_date";
    public static final String LIFECYCLE_STEP_WAIT_TIME_THRESHOLD = "index.lifecycle.step.wait_time_threshold";
    public static final String LIFECYCLE_HISTORY_INDEX_ENABLED = "indices.lifecycle.history_index_enabled";
    public static final String LIFECYCLE_STEP_MASTER_TIMEOUT = "indices.lifecycle.step.master_timeout";

    public static final String SLM_HISTORY_INDEX_ENABLED = "slm.history_index_enabled";
    public static final String SLM_RETENTION_SCHEDULE = "slm.retention_schedule";
    public static final String SLM_RETENTION_DURATION = "slm.retention_duration";
    public static final String SLM_MINIMUM_INTERVAL = "slm.minimum_interval";

    // This is not a setting configuring ILM per se, but certain ILM actions need to validate the managed index is not
    // already mounted as a searchable snapshot. Those ILM actions will check if the index has this setting name configured.
    public static final String SNAPSHOT_INDEX_NAME = "index.store.snapshot.index_name";

    public static final Setting<TimeValue> LIFECYCLE_POLL_INTERVAL_SETTING = Setting.timeSetting(
        LIFECYCLE_POLL_INTERVAL,
        TimeValue.timeValueMinutes(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> LIFECYCLE_NAME_SETTING = Setting.simpleString(
        LIFECYCLE_NAME,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Boolean> LIFECYCLE_INDEXING_COMPLETE_SETTING = Setting.boolSetting(
        LIFECYCLE_INDEXING_COMPLETE,
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Long> LIFECYCLE_ORIGINATION_DATE_SETTING = Setting.longSetting(
        LIFECYCLE_ORIGINATION_DATE,
        -1,
        -1,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Boolean> LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING = Setting.boolSetting(
        LIFECYCLE_PARSE_ORIGINATION_DATE,
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Boolean> LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING = Setting.boolSetting(
        LIFECYCLE_HISTORY_INDEX_ENABLED,
        true,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        LIFECYCLE_STEP_MASTER_TIMEOUT,
        TimeValue.timeValueSeconds(30),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );
    // This setting configures how much time since step_time should ILM wait for a condition to be met. After the threshold wait time has
    // elapsed ILM will likely stop waiting and go to the next step.
    // Also see {@link org.elasticsearch.xpack.core.ilm.ClusterStateWaitUntilThresholdStep}
    public static final Setting<TimeValue> LIFECYCLE_STEP_WAIT_TIME_THRESHOLD_SETTING = Setting.timeSetting(
        LIFECYCLE_STEP_WAIT_TIME_THRESHOLD,
        TimeValue.timeValueHours(12),
        TimeValue.timeValueHours(1),
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Boolean> SLM_HISTORY_INDEX_ENABLED_SETTING = Setting.boolSetting(
        SLM_HISTORY_INDEX_ENABLED,
        true,
        Setting.Property.NodeScope
    );
    public static final Setting<String> SLM_RETENTION_SCHEDULE_SETTING = Setting.simpleString(
        SLM_RETENTION_SCHEDULE,
        // Default to 1:30am every day
        "0 30 1 * * ?",
        str -> {
            try {
                if (Strings.hasText(str)) {
                    // Test that the setting is a valid cron syntax
                    new CronSchedule(str);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "invalid cron expression [" + str + "] for SLM retention schedule [" + SLM_RETENTION_SCHEDULE + "]",
                    e
                );
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> SLM_RETENTION_DURATION_SETTING = Setting.timeSetting(
        SLM_RETENTION_DURATION,
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMillis(500),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> SLM_MINIMUM_INTERVAL_SETTING = Setting.positiveTimeSetting(
        SLM_MINIMUM_INTERVAL,
        TimeValue.timeValueMinutes(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
}
