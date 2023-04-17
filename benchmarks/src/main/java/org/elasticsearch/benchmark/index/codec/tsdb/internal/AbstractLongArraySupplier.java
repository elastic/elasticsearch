/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import java.util.function.Supplier;

public abstract class AbstractLongArraySupplier implements Supplier<long[]> {

    protected final int bitsPerValue;
    protected final int size;

    public AbstractLongArraySupplier(int bitsPerValue, int size) {
        this.bitsPerValue = bitsPerValue;
        this.size = size;
    }
}
