/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

/**
 *  Bulk processor configuration.
 *  The bulk processor settings are grouped under xpack.applications.behavioral_analytics.ingest.bulk_processor.
 *  - flush_delay: the maximum delay between two flushes (default: 10s.)
 *  - max_events_per_bulk: the maximum number of events that can be added to the bulk before flushing the bulk (default: 1000)
 *  - max_number_of_retries: the maximum number of retries when bulk execution fails (default: 3)
 */
public class BulkProcessorConfig {
    private static final String SETTING_ROOT_PATH = "xpack.applications.behavioral_analytics.ingest.bulk_processor";

    public static final Setting<TimeValue> FLUSH_DELAY_SETTING = Setting.timeSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "flush_delay"),
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_NUMBER_OF_EVENTS_PER_BULK_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "max_events_per_bulk"),
        1000,
        1,
        10000,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_NUMBER_OF_RETRIES_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "max_number_of_retries"),
        3,
        0,
        5,
        Setting.Property.NodeScope
    );

    private final TimeValue flushDelay;

    private final int maxNumberOfRetries;

    private final int maxNumberOfEventsPerBulk;

    @Inject
    public BulkProcessorConfig(Settings settings) {
        this.flushDelay = FLUSH_DELAY_SETTING.get(settings);
        this.maxNumberOfRetries = MAX_NUMBER_OF_RETRIES_SETTING.get(settings);
        this.maxNumberOfEventsPerBulk = MAX_NUMBER_OF_EVENTS_PER_BULK_SETTING.get(settings);
    }

    public TimeValue flushDelay() {
        return flushDelay;
    }

    public int maxNumberOfRetries() {
        return maxNumberOfRetries;
    }

    public int maxNumberOfEventsPerBulk() {
        return maxNumberOfEventsPerBulk;
    }
}
