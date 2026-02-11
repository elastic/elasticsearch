/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.health;

public enum IndexCountLevel {
    NOT_EVALUATED("The index count level was not computed. For Internal Use Only."),
    PASS(""),
    NUDGE(
        "To ensure the best performance, we recommend grouping related data into fewer, "
            + "larger indices. For time-based data, consider using a data stream."
    ),
    WARN(
        "Your project is approaching an operational limit due to a high number of indices. "
            + "To avoid service interruptions, please review your indexing strategy."
    ),
    CRITICAL(
        "CRITICAL: Your project is about to reach its index limit. "
            + "Further index creation may be blocked. You must reduce your number of indices immediately."
    ),
    BLOCK(
        "Too Many Indices. Your project has reached its operational limit for the number of indices it can contain. "
            + "Please consolidate smaller indices or delete unused ones."
    );

    private final String message;

    IndexCountLevel(String message) {
        this.message = message;
    }

    public String getMessage() {
        return this.message;
    }

    public static IndexCountLevel parse(int totalUserIndices, int nudge, int warn, int critical, int block) {
        if (totalUserIndices < nudge) {
            return PASS;
        } else if (totalUserIndices < warn) {
            return NUDGE;
        } else if (totalUserIndices < critical) {
            return WARN;
        } else if (totalUserIndices < block) {
            return CRITICAL;
        } else {
            return BLOCK;
        }
    }
}
