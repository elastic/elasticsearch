/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.util.Locale;
import java.util.Map;

/**
 * Processor that converts the content of string fields to lowercase.
 * Throws exception is the field is not of type string.
 */

public final class LowercaseProcessor extends AbstractStringProcessor<String> {

    public static final String TYPE = "lowercase";

    LowercaseProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField) {
        super(processorTag, description, ignoreMissing, targetField, field);
    }

    public static String apply(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    @Override
    protected String process(String value) {
        return apply(value);
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
        protected LowercaseProcessor newProcessor(
            String tag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        ) {
            return new LowercaseProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
