/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class CalibrationUtilsTests extends ESTestCase {

    public void testToHeapDenseRoundTrip() throws IOException {
        float[][] data = { { 1f, 2f, 3f }, { 4f, 5f, 6f }, { 7f, 8f, 9f } };
        FloatVectorValues dense = KMeansFloatVectorValues.build(List.of(data[0].clone(), data[1].clone(), data[2].clone()), null, 3);
        FloatVectorValues again = CalibrationUtils.toHeapDenseFloatVectorValues(dense);
        assertEquals(3, again.size());
        assertEquals(3, again.dimension());
        assertArrayEquals(data[2], again.vectorValue(2), 0f);
        assertArrayEquals(data[0], again.vectorValue(0), 0f);
    }

    public void testToHeapDenseFromSequentialOnlyDelegate() throws IOException {
        float[][] vecs = { { 1f, 0f }, { 0f, 1f }, { 2f, 2f } };
        FloatVectorValues strict = sequentialOnlyFloatVectorValues(vecs);
        assertThrows(IllegalStateException.class, () -> strict.vectorValue(0));

        FloatVectorValues heap = CalibrationUtils.toHeapDenseFloatVectorValues(strict);
        assertEquals(3, heap.size());
        assertEquals(2, heap.dimension());
        assertArrayEquals(new float[] { 2f, 2f }, heap.vectorValue(2), 0f);
        assertArrayEquals(new float[] { 1f, 0f }, heap.vectorValue(0), 0f);

        assertThrows(IllegalStateException.class, () -> strict.vectorValue(0));
    }

    public void testToHeapDenseEmpty() throws IOException {
        FloatVectorValues empty = new FloatVectorValues() {
            @Override
            public FloatVectorValues copy() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int dimension() {
                return 2;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public DocIndexIterator iterator() {
                return new DocIndexIterator() {
                    @Override
                    public int docID() {
                        return NO_MORE_DOCS;
                    }

                    @Override
                    public int nextDoc() {
                        return NO_MORE_DOCS;
                    }

                    @Override
                    public int advance(int target) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }

                    @Override
                    public int index() {
                        return -1;
                    }
                };
            }

            @Override
            public float[] vectorValue(int ord) {
                throw new UnsupportedOperationException();
            }
        };
        FloatVectorValues heap = CalibrationUtils.toHeapDenseFloatVectorValues(empty);
        assertEquals(0, heap.size());
        assertEquals(2, heap.dimension());
    }

    /**
     * Mimics Lucene merged float vectors: {@code vectorValue(ord)} is only legal for the ordinal
     * most recently returned by {@link DocIndexIterator#index()} after {@link DocIndexIterator#nextDoc()}.
     */
    private static FloatVectorValues sequentialOnlyFloatVectorValues(float[][] vecs) {
        return new FloatVectorValues() {
            private int lastOrd = -1;

            @Override
            public FloatVectorValues copy() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int dimension() {
                return vecs[0].length;
            }

            @Override
            public int size() {
                return vecs.length;
            }

            @Override
            public DocIndexIterator iterator() {
                return new DocIndexIterator() {
                    private int nextOrd = 0;
                    private int docId = -1;

                    @Override
                    public int docID() {
                        return docId;
                    }

                    @Override
                    public int nextDoc() {
                        if (nextOrd >= vecs.length) {
                            docId = NO_MORE_DOCS;
                            return NO_MORE_DOCS;
                        }
                        lastOrd = nextOrd;
                        docId = nextOrd;
                        nextOrd++;
                        return docId;
                    }

                    @Override
                    public int advance(int target) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long cost() {
                        return vecs.length;
                    }

                    @Override
                    public int index() {
                        return lastOrd;
                    }
                };
            }

            @Override
            public float[] vectorValue(int ord) {
                if (ord != lastOrd) {
                    throw new IllegalStateException("only sequential: ord=" + ord + " lastOrd=" + lastOrd);
                }
                return vecs[ord];
            }
        };
    }
}
