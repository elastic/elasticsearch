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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pattern converter to populate CustomMapFields in a pattern.
 * This is to be used with custom ElasticSearch log messages
 * It will only populate these if the event have message of type <code>ESLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "CustomMapFields")
@ConverterKeys({ "CustomMapFields" })
public final class CustomMapFieldsConverter extends LogEventPatternConverter {

    private final Set<String> excludedFields;

    public CustomMapFieldsConverter(Set<String> excludedFields) {
        super("CustomMapFields", "CustomMapFields");
        this.excludedFields = excludedFields;
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static CustomMapFieldsConverter newInstance(final Configuration config, final String[] options) {
        // NOTE: the options carry the fields the layout writes itself, which this converter must not emit again. They are split
        // here because a pattern parser may hand them over either as one comma separated option or as several
        final Set<String> excludedFields = options == null
            ? Set.of()
            : Arrays.stream(options)
                .flatMap(option -> Arrays.stream(option.split(",")))
                .map(String::trim)
                .filter(field -> field.isEmpty() == false)
                .collect(Collectors.toUnmodifiableSet());
        return new CustomMapFieldsConverter(excludedFields);
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage() instanceof ESLogMessage logMessage) {
            logMessage.addJsonNoBrackets(toAppendTo, excludedFields);
        }
    }
}
