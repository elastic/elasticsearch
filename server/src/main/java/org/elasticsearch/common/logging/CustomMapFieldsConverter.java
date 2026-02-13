/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.util.Chars;
import org.apache.logging.log4j.util.StringBuilders;

/**
 * Pattern converter to populate CustomMapFields in a pattern.
 * This is to be used with custom ElasticSearch log messages
 * It will only populate these if the event have message of type <code>ESLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "CustomMapFields")
@ConverterKeys({ "CustomMapFields" })
public final class CustomMapFieldsConverter extends LogEventPatternConverter {

    public CustomMapFieldsConverter() {
        super("CustomMapFields", "CustomMapFields");
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static CustomMapFieldsConverter newInstance(final Configuration config, final String[] options) {
        return new CustomMapFieldsConverter();
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage() instanceof ESLogMessage logMessage) {
            addJsonNoBrackets(logMessage, toAppendTo);
        }
    }

    /**
     * This method is used in order to support ESJsonLayout which replaces %CustomMapFields from a pattern with JSON fields
     * It is a modified version of {@link MapMessage#asJson(StringBuilder)} where the curly brackets are not added
     * @param sb a string builder where JSON fields will be attached
     */
    private void addJsonNoBrackets(ESLogMessage logMessage, StringBuilder sb) {
        var map = logMessage.getIndexedReadOnlyStringMap();
        for (int i = 0; i < map.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(Chars.DQUOTE);
            int start = sb.length();
            sb.append(map.getKeyAt(i));
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE).append(':').append(Chars.DQUOTE);
            start = sb.length();
            Object value = map.getValueAt(i);
            sb.append(value);
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE);
        }
    }
}
