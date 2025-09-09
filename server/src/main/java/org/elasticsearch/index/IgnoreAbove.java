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

public record IgnoreAbove(Integer value, int defaultValue) {

    public static final IgnoreAbove IGNORE_ABOVE_STANDARD_INDICES = IgnoreAbove.builder()
        .defaultValue(IGNORE_ABOVE_DEFAULT_STANDARD_INDICES)
        .build();

    public static final IgnoreAbove IGNORE_ABOVE_LOGSDB_INDICES = IgnoreAbove.builder()
        .defaultValue(IGNORE_ABOVE_DEFAULT_LOGSDB_INDICES)
        .build();

    public int get() {
        return value != null ? value : defaultValue;
    }

    public boolean isSet() {
        return value != defaultValue;
    }

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
        private int defaultValue;  // cannot be null, hence int

        private Builder() {}

        public Builder value(Integer value) {
            this.value = value;
            return this;
        }

        public Builder defaultValue(int defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public IgnoreAbove build() {
            return new IgnoreAbove(value, defaultValue);
        }
    }
}
