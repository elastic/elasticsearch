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

import java.util.Objects;

/**
 * This class models the ignore_above parameter in indices.
 */
public class IgnoreAbove {

    public static final IgnoreAbove IGNORE_ABOVE_STANDARD_INDICES = new IgnoreAbove(null, IndexMode.STANDARD, IndexVersion.current());
    public static final IgnoreAbove IGNORE_ABOVE_LOGSDB_INDICES = new IgnoreAbove(null, IndexMode.LOGSDB, IndexVersion.current());

    private final Integer value;
    private final Integer defaultValue;

    public IgnoreAbove(Integer value) {
        this(Objects.requireNonNull(value), null, null);
    }

    public IgnoreAbove(Integer value, IndexMode indexMode, IndexVersion indexCreatedVersion) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException("[ignore_above] must be positive, got [" + value + "]");
        }

        this.value = value;
        this.defaultValue = IndexSettings.getIgnoreAboveDefaultValue(indexMode, indexCreatedVersion);
    }

    public int get() {
        return value != null ? value : defaultValue;
    }

    /**
     * Returns whether ignore_above is set; at field or index level.
     */
    public boolean isSet() {
        // if ignore_above equals default, its not considered to be set, even if it was explicitly set to the default value
        return Integer.valueOf(get()).equals(defaultValue) == false;
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

}
