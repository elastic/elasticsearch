/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BulkProcessorConfigTests extends ESTestCase {

    public void testDefaultConfig() {
        BulkProcessorConfig config = new BulkProcessorConfig(Settings.EMPTY);

        assertThat(config.flushDelay(), equalTo(TimeValue.timeValueSeconds(10)));
        assertThat(config.maxNumberOfEventsPerBulk(), equalTo(1000));
        assertThat(config.maxNumberOfRetries(), equalTo(3));
    }

    public void testCustomFlushDelay() {
        String value = randomTimeValue(1, 60, "s");
        BulkProcessorConfig config = createCustomConfig("flush_delay", value);
        assertThat(config.flushDelay(), equalTo(TimeValue.parseTimeValue(value, "flush_delay")));
    }

    public void testCustomFlushDelayTooLow() {
        String value = randomTimeValue(1, 60, "ms");
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("flush_delay", value));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.flush_delay], must be >= [1s]")
        );
    }

    public void testCustomFlushDelayTooHigh() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("flush_delay", "61s"));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.flush_delay], must be <= [60s]")
        );
    }

    public void testCustomMaxNumberOfEventsPerBulk() {
        int value = randomIntBetween(1, 10000);
        BulkProcessorConfig config = createCustomConfig("max_events_per_bulk", String.valueOf(value));
        assertThat(config.maxNumberOfEventsPerBulk(), equalTo(value));
    }

    public void testCustomMaxNumberOfEventsPerBulkTooLow() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_events_per_bulk", "0"));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.max_events_per_bulk] must be >= 1")
        );
    }

    public void testCustomMaxNumberOfEventsPerBulkTooHigh() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_events_per_bulk", "10001"));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.max_events_per_bulk] must be <= 10000")
        );
    }

    public void testCustomMaxNumberOfRetries() {
        int value = randomIntBetween(1, 5);
        BulkProcessorConfig config = createCustomConfig("max_number_of_retries", String.valueOf(value));
        assertThat(config.maxNumberOfRetries(), equalTo(value));
    }

    public void testCustomMaxNumberOfRetriesTooLow() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_number_of_retries", "-1"));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.max_number_of_retries] must be >= 0")
        );
    }

    public void testCustomMaxNumberOfRetriesTooHigh() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createCustomConfig("max_number_of_retries", "6"));
        assertThat(
            e.getMessage(),
            containsString("[xpack.applications.behavioral_analytics.ingest.bulk_processor.max_number_of_retries] must be <= 5")
        );
    }

    private BulkProcessorConfig createCustomConfig(String key, String value) {
        key = "xpack.applications.behavioral_analytics.ingest.bulk_processor." + key;
        return new BulkProcessorConfig(Settings.builder().put(key, value).build());
    }
}
