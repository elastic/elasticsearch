/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;

/**
 * Immutable context holding the calibration inputs that are invariant for an entire calibration run:
 * the similarity metric, vector data, query set, and retrieval target {@code k}. A single
 * {@code CalibrationSource} is constructed once and passed into both {@link ErrorModel} and
 * {@link ManifoldModel}, eliminating repeated threading of the same 10 parameters through every
 * call on the stack.
 *
 * <p>{@code vectors} is a single {@link FloatVectorValues} from which both query and corpus
 * vectors are read via disjoint ordinal sets ({@code queryOrdinals} and {@code corpusOrdinals}).
 *
 * <p>{@code baseDim} is the raw embedding dimension. {@code dim} is the working dimension used
 * for KMeans, scratch allocation, and quantization: equal to {@code baseDim} normally, or
 * {@code baseDim + 1} when {@code neyshabur} is {@code true} (Neyshabur lift appends a zero
 * component to each query before preconditioner application).
 */
public record CalibrationSource(
    VectorSimilarityFunction similarityFunction,
    int dim,
    FloatVectorValues vectors,
    int[] queryOrdinals,
    int baseDim,
    boolean cosine,
    boolean neyshabur,
    Preconditioner preconditioner,
    int[] corpusOrdinals,
    int k
) {}
