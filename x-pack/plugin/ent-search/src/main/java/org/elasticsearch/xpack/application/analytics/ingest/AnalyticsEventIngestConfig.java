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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

/**
 *  Bulk processor configuration.
 *  The bulk processor settings are grouped under xpack.applications.behavioral_analytics.ingest.bulk_processor.
 *  - flush_delay: the maximum delay between two flushes (default: 10s.)
 *  - max_events_per_bulk: the maximum number of events that can be added to the bulk before flushing the bulk (default: 1000)
 *  - max_number_of_retries: the maximum number of retries when bulk execution fails (default: 3)
 */
public class AnalyticsEventIngestConfig {
    private static final String SETTING_ROOT_PATH = "xpack.applications.behavioral_analytics.ingest";

    private static final TimeValue DEFAULT_FLUSH_DELAY = TimeValue.timeValueSeconds(10);
    private static final TimeValue MIN_FLUSH_DELAY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FLUSH_DELAY = TimeValue.timeValueSeconds(60);
    public static final Setting<TimeValue> FLUSH_DELAY_SETTING = Setting.timeSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "bulk_processor.flush_delay"),
        DEFAULT_FLUSH_DELAY,
        MIN_FLUSH_DELAY,
        MAX_FLUSH_DELAY,
        Setting.Property.NodeScope
    );

    private static final int DEFAULT_BULK_SIZE = 500;
    private static final int MIN_BULK_SIZE = 1;
    private static final int MAX_BULK_SIZE = 1000;
    public static final Setting<Integer> MAX_NUMBER_OF_EVENTS_PER_BULK_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "bulk_processor.max_events_per_bulk"),
        DEFAULT_BULK_SIZE,
        MIN_BULK_SIZE,
        MAX_BULK_SIZE,
        Setting.Property.NodeScope
    );

    private static final int DEFAULT_MAX_NUMBER_OF_RETRIES = 1;
    private static final int MIN_MAX_NUMBER_OF_RETRIES = 0;
    private static final int MAX_MAX_NUMBER_OF_RETRIES = 5;
    public static final Setting<Integer> MAX_NUMBER_OF_RETRIES_SETTING = Setting.intSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "bulk_processor.max_number_of_retries"),
        DEFAULT_MAX_NUMBER_OF_RETRIES,
        MIN_MAX_NUMBER_OF_RETRIES,
        MAX_MAX_NUMBER_OF_RETRIES,
        Setting.Property.NodeScope
    );

    private static final String DEFAULT_MAX_BYTES_IN_FLIGHT = "5%";
    public static final Setting<ByteSizeValue> MAX_BYTES_IN_FLIGHT_SETTING = Setting.memorySizeSetting(
        Strings.format("%s.%s", SETTING_ROOT_PATH, "bulk_processor.max_bytes_in_flight"),
        DEFAULT_MAX_BYTES_IN_FLIGHT,
        Setting.Property.NodeScope
    );

    private final TimeValue flushDelay;

    private final int maxNumberOfRetries;

    private final int maxNumberOfEventsPerBulk;

    private final ByteSizeValue maxBytesInFlight;

    @Inject
    public AnalyticsEventIngestConfig(Settings settings) {
        this.flushDelay = FLUSH_DELAY_SETTING.get(settings);
        this.maxNumberOfRetries = MAX_NUMBER_OF_RETRIES_SETTING.get(settings);
        this.maxNumberOfEventsPerBulk = MAX_NUMBER_OF_EVENTS_PER_BULK_SETTING.get(settings);
        this.maxBytesInFlight = MAX_BYTES_IN_FLIGHT_SETTING.get(settings);
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

    public ByteSizeValue maxBytesInFlight() {
        return maxBytesInFlight;
    }
}
