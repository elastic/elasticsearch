/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.stats;

import java.util.Locale;

public enum FeatureMetric {
    SEQUENCE,
    JOIN,
    EVENT,
    UNTIL,
    HEAD,
    TAIL,
    SEQUENCE_MAXSPAN,
    SEQUENCE_TWO_QUERIES,
    SEQUENCE_THREE_QUERIES,
    SEQUENCE_FOUR_QUERIES,
    SEQUENCE_FIVE_OR_MORE_QUERIES,
    JOIN_TWO_QUERIES,
    JOIN_THREE_QUERIES,
    JOIN_FOUR_QUERIES,
    JOIN_FIVE_OR_MORE_QUERIES,
    JOIN_KEYS_ONE,
    JOIN_KEYS_TWO,
    JOIN_KEYS_THREE,
    JOIN_KEYS_FOUR,
    JOIN_KEYS_FIVE_OR_MORE;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
