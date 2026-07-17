/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.ByteVectorValues;

import java.io.IOException;

/**
 * A {@link ByteVectorValues} that also implements {@link ClusteringVectorValues} for {@code byte[]},
 * so that the same instance can be used with the generic k-means infrastructure.
 */
public abstract sealed class ClusteringByteVectorValues extends ByteVectorValues implements ClusteringVectorValues<byte[]> permits
    KMeansByteVectorValues, ConcatenatedClusteringByteVectorValues {

    @Override
    public abstract ClusteringByteVectorValues copy() throws IOException;
}
