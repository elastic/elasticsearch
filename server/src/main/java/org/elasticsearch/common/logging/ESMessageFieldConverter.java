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
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.common.Strings;

/**
 * Pattern converter to populate ESMessageField in a pattern.
 * It will only populate these if the event have message of type <code>ESLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "ESMessageField")
@ConverterKeys({ "ESMessageField" })
public final class ESMessageFieldConverter extends LogEventPatternConverter {

    private String key;

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ESMessageFieldConverter newInstance(final Configuration config, final String[] options) {
        final String key = options[0];

        return new ESMessageFieldConverter(key);
    }

    public ESMessageFieldConverter(String key) {
        super("ESMessageField", "ESMessageField");
        this.key = key;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage() instanceof ESLogMessage) {
            ESLogMessage logMessage = (ESLogMessage) event.getMessage();
            final String value = logMessage.getValueFor(key);
            if (Strings.isNullOrEmpty(value) == false) {
                StringBuilders.appendValue(toAppendTo, value);
                return;
            }
        }
        StringBuilders.appendValue(toAppendTo, "");
    }
}
