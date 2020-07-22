/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql.stats;

import java.util.Locale;

public enum FeatureMetric {
    SEQUENCE,
    JOIN,
    EVENT,
    SEQUENCE_MAXSPAN,
    SEQUENCE_UNTIL,
    SEQUENCE_QUERIES_TWO,
    SEQUENCE_QUERIES_THREE,
    SEQUENCE_QUERIES_FOUR,
    SEQUENCE_QUERIES_FIVE_OR_MORE,
    JOIN_QUERIES_TWO,
    JOIN_QUERIES_THREE,
    JOIN_QUERIES_FOUR,
    JOIN_QUERIES_FIVE_OR_MORE,
    JOIN_UNTIL,
    JOIN_KEYS_ONE,
    JOIN_KEYS_TWO,
    JOIN_KEYS_THREE,
    JOIN_KEYS_FOUR,
    JOIN_KEYS_FIVE_OR_MORE,
    PIPE_HEAD,
    PIPE_TAIL;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
