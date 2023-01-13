/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores an array of long values.
 */
public sealed interface LongVector extends Vector permits ConstantLongVector,FilterLongVector,LongArrayVector {

    long getLong(int position);

    @Override
    LongVector filter(int... positions);

}
