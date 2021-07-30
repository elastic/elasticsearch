/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.BitSet;
import java.util.Iterator;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public final class FunctionTestUtils {

    public static Literal l(Object value) {
        return new Literal(EMPTY, value, DataTypes.fromJava(value));
    }

    public static Literal l(Object value, DataType type) {
        return new Literal(EMPTY, value, type);
    }

    public static Literal randomStringLiteral() {
        return l(ESTestCase.randomRealisticUnicodeOfLength(10), KEYWORD);
    }

    public static Literal randomIntLiteral() {
        return l(ESTestCase.randomInt(), INTEGER);
    }

    public static Literal randomBooleanLiteral() {
        return l(ESTestCase.randomBoolean(), BOOLEAN);
    }

    public static Literal randomDatetimeLiteral() {
        return l(ZonedDateTime.ofInstant(Instant.ofEpochMilli(ESTestCase.randomLong()), ESTestCase.randomZone()), DATETIME);
    }

    public static class Combinations implements Iterable<BitSet> {
        private int n;
        private int k;

        public Combinations(int n, int k) {
            this.n = n;
            this.k = k;
        }

        @Override
        public Iterator<BitSet> iterator() {
            return new Iterator<>() {
                BitSet bs = new BitSet(n);

                {
                    bs.set(0, k);
                }

                @Override
                public boolean hasNext() {
                    return bs != null;
                }

                @Override
                public BitSet next() {
                    BitSet old = (BitSet) bs.clone();
                    int b = bs.previousClearBit(n - 1);
                    int b1 = bs.previousSetBit(b);
                    if (b1 == -1) {
                        bs = null;
                    } else {
                        bs.clear(b1);
                        bs.set(b1 + 1, b1 + (n - b) + 1);
                        bs.clear(b1 + (n - b) + 1, n);
                    }
                    return old;
                }
            };
        }
    }
}
