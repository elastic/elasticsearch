/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;

import java.util.HashSet;
import java.util.Set;

public record OrStringConstraint(StringConstraint first, StringConstraint second) implements StringConstraint {

    @Override
    public OperatorType getType() {
        return OperatorType.OR;
    }

    @Override
    public char[] getValidCharacters() {
        char[] thisChars = first.getValidCharacters();
        char[] otherChars = second.getValidCharacters();
        if (thisChars == null || otherChars == null) {
            return null;
        }
        // return the union of valid characters
        Set<Character> validChars = new HashSet<>();
        for (char c : thisChars) {
            validChars.add(c);
        }
        for (char c : otherChars) {
            validChars.add(c);
        }
        return validChars.stream()
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString()
            .toCharArray();
    }

    @Override
    public boolean isApplicable(String value) {
        return first.isApplicable(value) || second.isApplicable(value);
    }
}
