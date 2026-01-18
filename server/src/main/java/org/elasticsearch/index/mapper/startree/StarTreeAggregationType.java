/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.startree;

import java.util.Locale;

/**
 * Supported aggregation types for star-tree values.
 */
public enum StarTreeAggregationType {
    SUM,
    COUNT,
    MIN,
    MAX;

    public static StarTreeAggregationType fromString(String value) {
        try {
            return StarTreeAggregationType.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Unknown star-tree aggregation type [" + value + "]. Valid values are: [SUM, COUNT, MIN, MAX]"
            );
        }
    }

    public String toXContentValue() {
        return name();
    }
}
