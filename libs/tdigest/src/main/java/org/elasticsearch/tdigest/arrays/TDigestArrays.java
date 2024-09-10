/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest.arrays;

/**
 * Minimal interface for BigArrays-like classes used within TDigest.
 */
public interface TDigestArrays {
    TDigestDoubleArray newDoubleArray(int initialSize);

    TDigestIntArray newIntArray(int initialSize);
}
