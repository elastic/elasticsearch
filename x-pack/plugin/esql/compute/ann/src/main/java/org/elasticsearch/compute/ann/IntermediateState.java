/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

/**
 * Description of a column of data that makes up the intermediate state of
 * an aggregation.
 */
public @interface IntermediateState {
    /**
     * Name of the column.
     */
    String name();

    /**
     * Type of the column. This should be the name of an element type or
     * an element type followed by {@code _BLOCK}. If this ends in {@code _BLOCK}
     * then the aggregation will the {@code Block} as an argument to
     * {@code combineIntermediate} and a position. It's the aggregation's
     * responsibility to iterate values from the block as needed.
     */
    String type();
}
