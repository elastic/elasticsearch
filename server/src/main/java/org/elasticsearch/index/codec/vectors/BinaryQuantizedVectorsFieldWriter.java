/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;

/**
 * Abstract base for field writers that accumulate float vectors and produce
 * binary-quantized output.  Concrete implementations decide how vectors are
 * stored in memory (e.g. on-heap ArrayList vs contiguous MemorySegment).
 */
public abstract class BinaryQuantizedVectorsFieldWriter extends FlatFieldVectorsWriter<float[]> {

    public abstract FieldInfo fieldInfo();

    public abstract void normalizeVectors();

    public abstract int getVectorCount();

    public abstract float[] dimensionSums();

    public abstract float[] getOnHeapVector(int i);
}
