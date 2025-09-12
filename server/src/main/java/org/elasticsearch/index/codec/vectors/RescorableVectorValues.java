/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

public interface RescorableVectorValues {

    VectorReScorer rescorer(float[] target) throws IOException;

    interface VectorReScorer {
        DocIdSetIterator iterator();

        Bulk bulk(DocIdSetIterator matchingDocs) throws IOException;

        interface Bulk {
            float nextDocsAndScores(int nextCount, Bits liveDocs, DocAndFloatFeatureBuffer buffer) throws IOException;
        }
    }

    final class DocAndFloatFeatureBuffer {

        private static final float[] EMPTY_FLOATS = new float[0];

        public int[] docs = IntsRef.EMPTY_INTS;
        public float[] features = EMPTY_FLOATS;
        public int size;

        public DocAndFloatFeatureBuffer() {}

        public void growNoCopy(int minSize) {
            if (docs.length < minSize) {
                docs = ArrayUtil.growNoCopy(docs, minSize);
                features = new float[docs.length];
            }
        }

        public void apply(Bits liveDocs) {
            int newSize = 0;
            for (int i = 0; i < size; ++i) {
                if (liveDocs.get(docs[i])) {
                    docs[newSize] = docs[i];
                    features[newSize] = features[i];
                    newSize++;
                }
            }
            this.size = newSize;
        }
    }

}
