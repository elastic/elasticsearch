/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

public interface StringPattern {
    /**
     * Returns the pattern in (Java) regex format.
     */
    String asJavaRegex();

    /**
     * Returns the pattern as a string. Should handle escaping.
     */
    String asString();

    /**
     * Hint method on whether this pattern matches everything or not.
     */
    default boolean matchesAll() {
        return false;
    }

    /**
     * Hint method on whether this pattern is exact, that is has no wildcard
     * or other patterns inside.
     */
    default boolean isExactMatch() {
        return false;
    }
}
