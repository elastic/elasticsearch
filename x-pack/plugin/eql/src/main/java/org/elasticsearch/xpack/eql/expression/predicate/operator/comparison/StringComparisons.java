/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

/**
 * EQL specific string comparison utilities.
 */
public final class StringComparisons {

    private StringComparisons() {}

    static Boolean insensitiveEquals(Object l, Object r) {
        if (l instanceof String && r instanceof String) {
            return ((String) l).compareToIgnoreCase((String) r) == 0;
        }
        if (l == null || r == null) {
            return null;
        }
        throw new EqlIllegalArgumentException("Insensitive comparison can be applied only on strings");
    }

    static Boolean insensitiveNotEquals(Object l, Object r) {
        Boolean equal = insensitiveEquals(l, r);
        return equal == null ? null : (equal == Boolean.TRUE ? Boolean.FALSE : Boolean.TRUE);
    }
}
