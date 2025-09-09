/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.xcontent.XContentString;

import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_DEFAULT_LOGSDB_INDICES;
import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_DEFAULT_STANDARD_INDICES;

/**
 * This class models the ignore_above parameter in indices.
 */
public class IgnoreAbove {

    public static final IgnoreAbove IGNORE_ABOVE_STANDARD_INDICES = IgnoreAbove.builder()
        .defaultValue(IGNORE_ABOVE_DEFAULT_STANDARD_INDICES)
        .build();

    public static final IgnoreAbove IGNORE_ABOVE_LOGSDB_INDICES = IgnoreAbove.builder()
        .defaultValue(IGNORE_ABOVE_DEFAULT_LOGSDB_INDICES)
        .build();

    private final Integer value;
    private final Integer defaultValue;

    public IgnoreAbove(Integer value, Integer defaultValue) {
        if (value == null && defaultValue == null) {
            throw new IllegalArgumentException(
                "IgnoreAbove must be initialized with at least one non-null argument: either 'value' or 'defaultValue'"
            );
        }
        this.value = value;
        this.defaultValue = defaultValue;
    }

    public int get() {
        return value != null ? value : defaultValue;
    }

    /**
     * Returns whether ignore_above is set; at field or index level.
     */
    public boolean isSet() {
        // if ignore_above equals default, its not considered to be set, even if it was explicitly set to the default value
        return value != null && value.equals(defaultValue) == false;
    }

    /**
     * Returns whether the given string will be ignored.
     */
    public boolean isIgnored(final String s) {
        if (s == null) return false;
        return lengthExceedsIgnoreAbove(s.length());
    }

    public boolean isIgnored(final XContentString s) {
        if (s == null) return false;
        return lengthExceedsIgnoreAbove(s.stringLength());
    }

    private boolean lengthExceedsIgnoreAbove(int strLength) {
        return strLength > get();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Integer value;
        private Integer defaultValue;

        private Builder() {}

        public Builder value(Integer value) {
            this.value = value;
            return this;
        }

        public Builder defaultValue(Integer defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public IgnoreAbove build() {
            return new IgnoreAbove(value, defaultValue);
        }
    }
}
