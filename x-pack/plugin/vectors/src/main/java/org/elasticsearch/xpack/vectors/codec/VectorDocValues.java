/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public abstract class VectorDocValues extends BinaryDocValues {
    final int maxDoc;
    int doc = -1;

    VectorDocValues(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    @Override
    public int advance(int target) throws IOException {
        if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
        }
        return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        doc = target;
        return true;
    }

    /*
     * get product centroids computed for all documents in this segment
     */
    public abstract float[][][] pcentroids () throws IOException;

    /*
     * get squared magnitudes of the product centroids computed for all documents in this segment
     */
    public abstract float[][] pCentroidsSquaredMagnitudes() throws IOException;

    /*
     * get number of product centroids in each product quantizer for this segment
     */
    public abstract int pcentroidsCount();


    /*
     * get a binary value of the product centroids that the current document belongs to
     */
    public abstract BytesRef docCentroids() throws IOException;

}
