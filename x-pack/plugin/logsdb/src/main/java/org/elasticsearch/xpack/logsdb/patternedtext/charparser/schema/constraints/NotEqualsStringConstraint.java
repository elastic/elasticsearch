/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;

public record NotEqualsStringConstraint(String targetValue) implements StringConstraint {

    @Override
    public OperatorType getType() {
        return OperatorType.NOT_EQUAL;
    }

    @Override
    public char[] getValidCharacters() {
        return null;
    }

    @Override
    public boolean isApplicable(String value) {
        return targetValue.equals(value) == false;
    }
}
