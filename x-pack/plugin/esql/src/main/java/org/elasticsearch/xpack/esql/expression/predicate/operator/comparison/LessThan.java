/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;

public class LessThan {
    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs < rhs;
    }

    @Evaluator(extraName = "Keywords")  // TODO rename to "Bytes"
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.compareTo(rhs) < 0;
    }
}
