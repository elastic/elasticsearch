/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.common.settings.Setting.timeSetting;

public final class MonitoringField {

    /**
     * The minimum amount of time allowed for the history duration.
     */
    public static final TimeValue HISTORY_DURATION_MINIMUM = TimeValue.timeValueHours(24);
    /**
     * The default retention duration of the monitoring history data.
     * <p>
     * Expected values:
     * <ul>
     * <li>Default: 7 days</li>
     * <li>Minimum: 1 day</li>
     * </ul>
     *
     * @see MonitoringField#HISTORY_DURATION_MINIMUM
     */
    public static final Setting<TimeValue> HISTORY_DURATION = timeSetting(
        "xpack.monitoring.history.duration",
        TimeValue.timeValueHours(7 * 24), // default value (7 days)
        HISTORY_DURATION_MINIMUM,         // minimum value
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private MonitoringField() {}
}
