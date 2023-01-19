/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector that stores double values.
 * This class is generated. Do not edit it.
 */
public sealed interface DoubleVector extends Vector permits ConstantDoubleVector,FilterDoubleVector,DoubleArrayVector {

    double getDouble(int position);

    @Override
    DoubleBlock asBlock();

    @Override
    DoubleVector filter(int... positions);

}
