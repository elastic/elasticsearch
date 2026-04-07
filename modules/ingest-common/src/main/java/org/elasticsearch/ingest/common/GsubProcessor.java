/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor that allows to search for patterns in field content and replace them with corresponding string replacement.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public final class GsubProcessor extends AbstractStringProcessor<String> {

    public static final String TYPE = "gsub";
    private static final Logger logger = LogManager.getLogger(GsubProcessor.class);

    private final Pattern pattern;
    private final String replacement;

    GsubProcessor(
        String tag,
        String description,
        String field,
        Pattern pattern,
        String replacement,
        boolean ignoreMissing,
        String targetField
    ) {
        super(tag, description, ignoreMissing, targetField, field);
        this.pattern = pattern;
        this.replacement = replacement;
    }

    Pattern getPattern() {
        return pattern;
    }

    String getReplacement() {
        return replacement;
    }

    @Override
    protected String process(String value) {
        try {
            return pattern.matcher(value).replaceAll(replacement);
        } catch (StackOverflowError e) {
            /*
             * A bad regex on problematic data can trigger a StackOverflowError. In this case we can safely recover from the
             * StackOverflowError, so we rethrow it as an Exception instead. This way the document fails this processor, but processing
             * can carry on. The value would be useful to log here, but we do not do so for because we do not want to write potentially
             * sensitive data to the logs.
             */
            String message = "Caught a StackOverflowError while processing gsub pattern: [" + pattern + "]";
            logger.trace(message, e);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractStringProcessor.Factory {

        public Factory() {
            super(TYPE);
        }

        @Override
        protected GsubProcessor newProcessor(
            String processorTag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        ) {
            String pattern = readStringProperty(TYPE, processorTag, config, "pattern");
            String replacement = readStringProperty(TYPE, processorTag, config, "replacement");
            Pattern searchPattern;
            try {
                searchPattern = Pattern.compile(pattern);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "pattern", "Invalid regex pattern. " + e.getMessage());
            }
            return new GsubProcessor(processorTag, description, field, searchPattern, replacement, ignoreMissing, targetField);
        }
    }
}
