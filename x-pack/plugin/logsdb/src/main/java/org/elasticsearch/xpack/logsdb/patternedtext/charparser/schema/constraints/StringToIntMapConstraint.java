/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;

import java.util.Map;

public record StringToIntMapConstraint(Map<String, Integer> map) implements StringConstraint {

    @Override
    public OperatorType getType() {
        return OperatorType.MAP;
    }

    @Override
    public char[] getValidCharacters() {
        return map.entrySet()
            .stream()
            .flatMap(entry -> entry.getKey().chars().mapToObj(c -> (char) c))
            .distinct()
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString()
            .toCharArray();
    }

    @Override
    public boolean isApplicable(String value) {
        return map.containsKey(value);
    }
}
