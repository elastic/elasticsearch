/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores an array of double values.
 */
public interface DoubleVector extends Vector {

    double getDouble(int position);

    @Override
    DoubleVector filter(int... positions);

}
