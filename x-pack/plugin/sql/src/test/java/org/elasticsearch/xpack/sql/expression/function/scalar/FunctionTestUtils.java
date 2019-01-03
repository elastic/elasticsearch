/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;

import java.util.BitSet;
import java.util.Iterator;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;

public final class FunctionTestUtils {

    public static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }

    public static Literal randomStringLiteral() {
        return l(ESTestCase.randomRealisticUnicodeOfLength(1024));
    }
    
    public static Literal randomIntLiteral() {
        return l(ESTestCase.randomInt());
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
            return new Iterator<BitSet>() {
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
                    if (b1 == -1)
                        bs = null;
                    else {
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
