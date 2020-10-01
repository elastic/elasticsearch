/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Comparisons;

/**
 * EQL specific string comparison utilities.
 */
public final class StringComparisons {

    private StringComparisons() {}

    static Boolean insensitiveEquals(Object l, Object r) {
        if (l instanceof String && r instanceof String) {
            return ((String)l).compareToIgnoreCase((String) r) == 0;
        }
        return Comparisons.eq(l, r);
    }

    static Boolean insensitiveNotEquals(Object l, Object r) {
        Boolean equal = insensitiveEquals(l, r);
        return equal == null ? null : (equal == Boolean.TRUE ? Boolean.FALSE : Boolean.TRUE);
    }
}
