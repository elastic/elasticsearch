/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class XSkipBlockRangeIteratorTests extends ESTestCase {

    public void testLuceneVersion() {
        assertFalse(
            "Remove this class after upgrading to Lucene 10.4",
            IndexVersion.current().luceneVersion().onOrAfter(org.apache.lucene.util.Version.fromBits(10, 4, 0))
        );
    }

    public void testSkipBlockRangeIterator() throws Exception {

        DocValuesSkipper skipper = docValuesSkipper(10, 20, true);
        XSkipBlockRangeIterator it = new XSkipBlockRangeIterator(skipper, 10, 20);

        assertEquals(0, it.nextDoc());
        assertEquals(256, it.docIDRunEnd());
        assertEquals(100, it.advance(100));
        assertEquals(768, it.advance(300));
        assertEquals(1024, it.docIDRunEnd());
        assertEquals(1100, it.advance(1100));
        assertEquals(1280, it.docIDRunEnd());
        assertEquals(1792, it.advance(1500));
        assertEquals(2048, it.docIDRunEnd());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(2050));
    }

    public void testIntoBitSet() throws Exception {
        DocValuesSkipper skipper = docValuesSkipper(10, 20, true);
        XSkipBlockRangeIterator it = new XSkipBlockRangeIterator(skipper, 10, 20);
        assertEquals(768, it.advance(300));
        FixedBitSet bitSet = new FixedBitSet(2048);
        it.intoBitSet(1500, bitSet, 768);

        FixedBitSet expected = new FixedBitSet(2048);
        expected.set(0, 512);

        assertEquals(expected, bitSet);
    }

    protected static NumericDocValues docValues(long queryMin, long queryMax) {
        return new NumericDocValues() {

            int doc = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                if (target < 1024) {
                    // dense up to 1024
                    return doc = target;
                } else if (doc < 2047) {
                    // 50% docs have a value up to 2048
                    return doc = target + (target & 1);
                } else {
                    return doc = DocIdSetIterator.NO_MORE_DOCS;
                }
            }

            @Override
            public long longValue() throws IOException {
                int d = doc % 1024;
                if (d < 128) {
                    return (queryMin + queryMax) >> 1;
                } else if (d < 256) {
                    return queryMax + 1;
                } else if (d < 512) {
                    return queryMin - 1;
                } else {
                    return switch ((d / 2) % 3) {
                        case 0 -> queryMin - 1;
                        case 1 -> queryMax + 1;
                        case 2 -> (queryMin + queryMax) >> 1;
                        default -> throw new AssertionError();
                    };
                }
            }

            @Override
            public long cost() {
                return 42;
            }
        };
    }

    /**
     * Fake skipper over a NumericDocValues field built by an equivalent call to {@link
     * #docValues(long, long)}
     */
    protected static DocValuesSkipper docValuesSkipper(long queryMin, long queryMax, boolean doLevels) {
        return new DocValuesSkipper() {

            int doc = -1;

            @Override
            public void advance(int target) throws IOException {
                doc = target;
            }

            @Override
            public int numLevels() {
                return doLevels ? 3 : 1;
            }

            @Override
            public int minDocID(int level) {
                int rangeLog = 9 - numLevels() + level;

                // the level is the log2 of the interval
                if (doc < 0) {
                    return -1;
                } else if (doc >= 2048) {
                    return DocIdSetIterator.NO_MORE_DOCS;
                } else {
                    int mask = (1 << rangeLog) - 1;
                    // prior multiple of 2^level
                    return doc & ~mask;
                }
            }

            @Override
            public int maxDocID(int level) {
                int rangeLog = 9 - numLevels() + level;

                int minDocID = minDocID(level);
                return switch (minDocID) {
                    case -1 -> -1;
                    case DocIdSetIterator.NO_MORE_DOCS -> DocIdSetIterator.NO_MORE_DOCS;
                    default -> minDocID + (1 << rangeLog) - 1;
                };
            }

            @Override
            @SuppressWarnings("DuplicateBranches")
            public long minValue(int level) {
                int d = doc % 1024;
                if (d < 128) {
                    return queryMin;
                } else if (d < 256) {
                    return queryMax + 1;
                } else if (d < 768) {
                    return queryMin - 1;
                } else {
                    return queryMin - 1;
                }
            }

            @Override
            public long maxValue(int level) {
                int d = doc % 1024;
                if (d < 128) {
                    return queryMax;
                } else if (d < 256) {
                    return queryMax + 1;
                } else if (d < 768) {
                    return queryMin - 1;
                } else {
                    return queryMax + 1;
                }
            }

            @Override
            public int docCount(int level) {
                int rangeLog = 9 - numLevels() + level;

                if (doc < 1024) {
                    return 1 << rangeLog;
                } else {
                    // half docs have a value
                    return 1 << rangeLog >> 1;
                }
            }

            @Override
            public long minValue() {
                return Long.MIN_VALUE;
            }

            @Override
            public long maxValue() {
                return Long.MAX_VALUE;
            }

            @Override
            public int docCount() {
                return 1024 + 1024 / 2;
            }
        };
    }
}
