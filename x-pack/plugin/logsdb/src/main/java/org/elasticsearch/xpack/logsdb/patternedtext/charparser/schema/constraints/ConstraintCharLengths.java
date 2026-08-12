/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

/**
 * Helpers for combining the character-length ({@code {n}}) of two constraints when they are chained with {@code &&} / {@code ||}. A
 * subToken may declare at most ONE character length: the numeric char-length gate stores a single length per subToken, so two different
 * lengths cannot be represented. Rather than silently mishandling {@code {4} && {6}} (a contradiction) or {@code {4} || {6}} (unsupported),
 * we reject them at schema-parse time. This applies uniformly to numeric ({@link IntConstraint}) and string ({@link StringConstraint})
 * subTokens.
 */
final class ConstraintCharLengths {

    private ConstraintCharLengths() {}

    /**
     * Combines the required character lengths of two chained constraints. A length of -1 means "no {n} constraint" and acts as the
     * identity. Two present but different lengths are a conflict and throw.
     */
    static int combine(int first, int second) {
        if (first >= 0 && second >= 0 && first != second) {
            throw new IllegalArgumentException(
                "A subToken may declare at most one character length {n}; found conflicting {" + first + "} and {" + second + "}"
            );
        }
        return first >= 0 ? first : second;
    }
}
