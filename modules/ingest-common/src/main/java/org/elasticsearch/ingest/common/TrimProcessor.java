/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.ConfigurationUtils;

import java.util.Locale;
import java.util.Map;

/**
 * Processor that trims the content of string fields.
 * Supports directional trimming (both, leading, or trailing whitespace).
 * Throws exception if the field is not of type string.
 */
public final class TrimProcessor extends AbstractStringProcessor<String> {

    public static final String TYPE = "trim";

    public enum TrimMode {
        BOTH {
            @Override
            String trim(String value) {
                return value.trim();
            }
        },
        LEADING {
            @Override
            String trim(String value) {
                int len = value.length();
                int st = 0;
                while ((st < len) && (value.charAt(st) <= ' ')) {
                    st++;
                }
                return st > 0 ? value.substring(st) : value;
            }
        },
        TRAILING {
            @Override
            String trim(String value) {
                int len = value.length();
                while ((len > 0) && (value.charAt(len - 1) <= ' ')) {
                    len--;
                }
                return len < value.length() ? value.substring(0, len) : value;
            }
        };

        abstract String trim(String value);

        public static TrimMode fromString(String processorTag, String propertyName, String mode) {
            try {
                return TrimMode.valueOf(mode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw ConfigurationUtils.newConfigurationException(
                    TYPE,
                    processorTag,
                    propertyName,
                    "trim mode [" + mode + "] not supported. Valid modes are: [both, leading, trailing]."
                );
            }
        }
    }

    private final TrimMode trimMode;

    TrimProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField) {
        this(processorTag, description, field, ignoreMissing, targetField, TrimMode.BOTH);
    }

    TrimProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField, TrimMode trimMode) {
        super(processorTag, description, ignoreMissing, targetField, field);
        this.trimMode = trimMode == null ? TrimMode.BOTH : trimMode;
    }

    public TrimMode getTrimMode() {
        return trimMode;
    }

    @Override
    protected String process(String value) {
        return trimMode.trim(value);
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
        protected TrimProcessor newProcessor(
            String tag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        ) {
            String modeStr = ConfigurationUtils.readStringProperty(TYPE, tag, config, "trim_mode", "both");
            TrimMode trimMode = TrimMode.fromString(tag, "trim_mode", modeStr);
            return new TrimProcessor(tag, description, field, ignoreMissing, targetField, trimMode);
        }
    }
}
