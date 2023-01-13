/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Vector implementation that stores BytesRef values.
 */
public sealed interface BytesRefVector extends Vector permits BytesRefArrayVector,ConstantBytesRefVector,FilterBytesRefVector {

    BytesRef getBytesRef(int position, BytesRef spare);

    @Override
    BytesRefVector filter(int... positions);
}
