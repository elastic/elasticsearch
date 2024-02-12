/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;

public class ConnectorUtils {

    /**
     * Parses a field from the XContentParser to an Instant. This method should be used for parsing
     * all datetime fields related to Connector APIs. It utilizes the parseTimeFieldToInstant method from {@link TimeUtils}
     * to parse the date-time string to an Instant.
     *
     * @param p         the XContentParser instance from which to parse the date-time string.
     * @param fieldName the name of the field whose value is to be parsed.
     */
    public static Instant parseInstant(XContentParser p, String fieldName) throws IOException {
        return TimeUtils.parseTimeFieldToInstant(p, fieldName);
    }

    /**
     * Parses a nullable field from the XContentParser to an Instant. This method is useful
     * when parsing datetime fields that might have null values.
     *
     * @param p         the XContentParser instance from which to parse the date-time string.
     * @param fieldName the name of the field whose value is to be parsed.
     */
    public static Instant parseNullableInstant(XContentParser p, String fieldName) throws IOException {
        return p.currentToken() == XContentParser.Token.VALUE_NULL ? null : parseInstant(p, fieldName);
    }
}
