/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;

public class AnalyticsEventLogger {
    private static final Logger logger = LogManager.getLogger(AnalyticsEventLogger.class);

    /**
     * Logs an analytics event as a JSON string.
     *
     * @param event the event to format
     * @throws IOException if an I/O error occurs while formatting the JSON
     */
    public void logEvent(AnalyticsEvent event) throws IOException {
        logger.info(formatEvent(event));
    }

    /**
     * Formats an analytics event as a JSON string.
     *
     * @param event the event to format
     *
     * @return the formatted JSON string
     *
     * @throws IOException if an I/O error occurs while formatting the JSON
     */
    private String formatEvent(AnalyticsEvent event) throws IOException {
        return Strings.toString(event.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));
    }
}
