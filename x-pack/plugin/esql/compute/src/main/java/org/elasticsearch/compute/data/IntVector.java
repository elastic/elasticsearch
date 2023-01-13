/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores int values.
 */
public sealed interface IntVector extends Vector permits ConstantIntVector,FilterIntVector,IntArrayVector {

    int getInt(int position);

    @Override
    IntBlock asBlock();

    @Override
    IntVector filter(int... positions);

}
