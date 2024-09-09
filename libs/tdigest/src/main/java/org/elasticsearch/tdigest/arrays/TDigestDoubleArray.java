/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest.arrays;

/**
 * Minimal interface for DoubleArray-like classes used within TDigest.
 */
public interface TDigestDoubleArray {
    int size();

    double get(int index);

    void set(int index, double value);

    void add(double value);

    void sorted();

    void ensureCapacity(int requiredCapacity);
}
