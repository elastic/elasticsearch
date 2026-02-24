/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.search.SearchLogProducer.SEARCH_LOGGER_LOG_SYSTEM;
import static org.elasticsearch.common.logging.activity.ActivityLogProducer.ES_FIELDS_PREFIX;
import static org.elasticsearch.common.logging.activity.ActivityLogProducer.EVENT_OUTCOME_FIELD;
import static org.elasticsearch.common.logging.activity.ActivityLogger.ACTIVITY_LOGGER_ENABLED;
import static org.elasticsearch.test.ESIntegTestCase.updateClusterSettings;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;

public class ActivityLoggingUtils {

    private static final String[] loggers = new String[] { "search", "sql", "esql", "eql" };

    public static void enableLoggers() {
        var builder = Settings.builder();
        for (String logger : loggers) {
            builder.put(ACTIVITY_LOGGER_ENABLED.getConcreteSettingForNamespace(logger).getKey(), true);
        }
        updateClusterSettings(builder);
    }

    public static void disableLoggers() {
        var builder = Settings.builder();
        for (String logger : loggers) {
            builder.put(ACTIVITY_LOGGER_ENABLED.getConcreteSettingForNamespace(logger).getKey(), (String) null);
        }
        updateClusterSettings(builder);
    }

    public static void enableLoggingSystem() {
        var builder = Settings.builder();
        builder.put(SEARCH_LOGGER_LOG_SYSTEM.getKey(), true);
        updateClusterSettings(builder);
    }

    public static void disableLoggingSystem() {
        var builder = Settings.builder();
        builder.put(SEARCH_LOGGER_LOG_SYSTEM.getKey(), (String) null);
        updateClusterSettings(builder);
    }

    public static Map<String, String> getMessageData(LogEvent event) {
        assertNotNull(event);
        assertThat(event.getMessage(), instanceOf(ESLogMessage.class));
        ESLogMessage message = (ESLogMessage) event.getMessage();
        HashMap<String, String> map = new HashMap<>();
        message.getData().forEach((k, v) -> map.put(k, v.toString()));
        return map;
    }

    public static void assertMessageSuccess(Map<String, String> message, String type, String query) {
        assertThat(message.get(EVENT_OUTCOME_FIELD), equalTo("success"));
        assertThat(message.get(ES_FIELDS_PREFIX + "type"), equalTo(type));
        assertThat(Long.valueOf(message.get(ES_FIELDS_PREFIX + "took")), greaterThan(0L));
        assertThat(Long.valueOf(message.get(ES_FIELDS_PREFIX + "took_millis")), greaterThanOrEqualTo(0L));
        assertThat(message.get(ES_FIELDS_PREFIX + "query"), containsString(query));
    }

    public static void assertMessageFailure(
        Map<String, String> message,
        String type,
        String query,
        Class<? extends Throwable> exception,
        String errorMessage
    ) {
        assertThat(message.get(EVENT_OUTCOME_FIELD), equalTo("failure"));
        assertThat(message.get(ES_FIELDS_PREFIX + "type"), equalTo(type));
        assertThat(Long.valueOf(message.get(ES_FIELDS_PREFIX + "took")), greaterThan(0L));
        assertThat(Long.valueOf(message.get(ES_FIELDS_PREFIX + "took_millis")), greaterThanOrEqualTo(0L));
        assertThat(message.get(ES_FIELDS_PREFIX + "query"), containsString(query));
        if (errorMessage != null) {
            assertThat(message.get("error.message"), containsString(errorMessage));
        }
        if (exception != null) {
            assertThat(message.get("error.type"), equalTo(exception.getName()));
        }
    }
}
