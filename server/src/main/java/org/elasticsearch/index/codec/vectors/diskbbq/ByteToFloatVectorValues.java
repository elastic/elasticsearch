/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;

/**
 * Adapts {@link ByteVectorValues} to {@link FloatVectorValues} by converting each byte value
 * ([-128, 127]) to the corresponding float ([-128.0, 127.0]). This allows byte-encoded vectors
 * to be processed through the float-based IVF pipeline (clustering, quantization, posting lists)
 * without modifying the IVF internals.
 * <p>
 * When {@code normalize} is true, each converted float vector is L2-normalized to unit length.
 * This is used for cosine similarity: the IVF pipeline treats normalized byte vectors identically
 * to dot-product, which is how the ES mapper handles cosine for float vectors.
 */
public class ByteToFloatVectorValues extends FloatVectorValues {

    private final ByteVectorValues delegate;
    private final float[] scratch;
    private final boolean normalize;

    public ByteToFloatVectorValues(ByteVectorValues delegate) {
        this(delegate, false);
    }

    public ByteToFloatVectorValues(ByteVectorValues delegate, boolean normalize) {
        this.delegate = delegate;
        this.scratch = new float[delegate.dimension()];
        this.normalize = normalize;
    }

    @Override
    public int dimension() {
        return delegate.dimension();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        byte[] bytes = delegate.vectorValue(ord);
        for (int i = 0; i < bytes.length; i++) {
            scratch[i] = bytes[i];
        }
        if (normalize) {
            VectorUtil.l2normalize(scratch);
        }
        return scratch;
    }

    @Override
    public int getVectorByteLength() {
        return delegate.dimension() * Float.BYTES;
    }

    @Override
    public DocIndexIterator iterator() {
        return delegate.iterator();
    }

    @Override
    public int ordToDoc(int ord) {
        return delegate.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
        return delegate.getAcceptOrds(acceptDocs);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
        // Not used by the IVF pipeline (which does its own quantized scoring). This method exists only
        // to satisfy the FloatVectorValues contract. The delegate's byte scorer uses Lucene's byte-scale
        // similarity formula, which differs from the float-scale formula used by IVF — do not use this
        // for scoring in the IVF pipeline.
        throw new UnsupportedOperationException("ByteToFloatVectorValues.scorer() should not be called in the IVF pipeline");
    }

    @Override
    public FloatVectorValues copy() throws IOException {
        return new ByteToFloatVectorValues(delegate.copy(), normalize);
    }
}
