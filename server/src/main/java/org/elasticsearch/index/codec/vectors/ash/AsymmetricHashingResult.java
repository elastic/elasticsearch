/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

/**
 * Result of ASH encoding for a set of vectors.
 *
 * @param w the learned projection matrix, shape (originalDim, nDims). Stored for query-time transformation.
 * @param encodedVectors quantized codes in the latent space, shape (nVectors, nDims). Values are centered floats.
 * @param scales per-vector scale factor (float16 precision), applied to reconstruct dot product magnitude
 * @param offsets per-vector offset correction (float16 precision), accounts for centroid dot product terms
 * @param nClusters number of clusters used (determines cluster_id bit width in header)
 */
public record AsymmetricHashingResult(float[][] w, float[][] encodedVectors, float[] scales, float[] offsets, int nClusters) {}
