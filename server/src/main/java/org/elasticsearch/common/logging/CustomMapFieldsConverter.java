/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

/**
 * Pattern converter to populate CustomMapFields in a pattern.
 * This is to be used with custom ElasticSearch log messages
 * It will only populate these if the event have message of type <code>ESLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "CustomMapFields")
@ConverterKeys({"CustomMapFields"})
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
        if(event.getMessage() instanceof ESLogMessage) {
            ESLogMessage logMessage = (ESLogMessage) event.getMessage();
            logMessage.addJsonNoBrackets(toAppendTo);
        }
    }
}
