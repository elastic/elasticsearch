/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.stats;

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

    private final String prefix;

    FeatureMetric() {
        String featureName = this.toString();
        String prefix = "features.";

        if (featureName.startsWith("sequence_")) {
            prefix += "sequences.";
        } else if (featureName.startsWith("join_k")) {
            prefix += "keys.";
        } else if (featureName.startsWith("join_")) {
            prefix += "joins.";
        } else if (featureName.startsWith("pipe_")) {
            prefix += "pipes.";
        }
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public String prefixedName() {
        return this.prefix + this.toString();
    }
}
