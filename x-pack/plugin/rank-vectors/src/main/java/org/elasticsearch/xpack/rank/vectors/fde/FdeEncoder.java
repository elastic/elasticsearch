/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.fde;

/**
 * Maps a variable-length set of token vectors to one fixed-dimensional vector whose inner
 * product with an encoded query approximates the late-interaction (MaxSim) score. This lets
 * an ordinary single-vector ANN index act as the first pass for a multi-vector field, with
 * exact MaxSim applied afterwards to the candidates.
 * <p>
 * Documents and queries are encoded asymmetrically, so the two methods are not interchangeable.
 * This interface is intended for internal measurements. Implementations do not provide a storage
 * or cross-version compatibility contract for their encoded vectors.
 */
public interface FdeEncoder {

    /** Length of the vectors returned by {@link #encodeDocument} and {@link #encodeQuery}. */
    int dimensions();

    /**
     * Encodes a document's token vectors.
     *
     * @param tokens one non-null, unit-normalized row per token, each of the configured token dimension
     * and containing only finite values; must be non-empty
     */
    float[] encodeDocument(float[][] tokens);

    /**
     * Encodes a query's token vectors.
     *
     * @param tokens one non-null, unit-normalized row per token, each of the configured token dimension
     * and containing only finite values; must be non-empty
     */
    float[] encodeQuery(float[][] tokens);
}
