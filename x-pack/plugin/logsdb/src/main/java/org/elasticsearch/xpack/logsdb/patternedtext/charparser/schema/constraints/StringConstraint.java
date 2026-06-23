/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;

public interface StringConstraint {

    /**
     * Returns the type of this constraint, which is one of the {@link OperatorType} values.
     * This method is used to determine how the constraint should be applied in logical operations.
     *
     * @return the operator type of this constraint
     */
    OperatorType getType();

    /**
     * Returns the characters that are valid for this constraint. For example, a Set constraint that includes the values "One", "Two"
     * and "Three" would return the characters {'O', 'n', 'e', 'T', 'w', 'o', 'h', 'r', 'e'}.
     * If the constraint does not have a specific set of valid characters, this method should return null.
     * If no characters are valid, this method should return an empty array.
     *
     * @return an array of valid characters for this constraint, or null if there are no specific valid characters
     */
    char[] getValidCharacters();

    /**
     * Evaluates the constraint against a given string value.
     *
     * @param value the string value to evaluate
     * @return true if the value satisfies the constraint, false otherwise
     */
    boolean isApplicable(String value);

    default StringConstraint and(final StringConstraint other) {
        return new AndStringConstraint(this, other);
    }

    default StringConstraint or(final StringConstraint other) {
        return new OrStringConstraint(this, other);
    }
}
