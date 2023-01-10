/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;

/**
 * Types used by the code generator.
 */
public class Types {
    private static final String PACKAGE = "org.elasticsearch.compute";
    private static final String AGGREGATION_PACKAGE = PACKAGE + ".aggregation";
    private static final String DATA_PACKAGE = PACKAGE + ".data";

    static final ClassName PAGE = ClassName.get(DATA_PACKAGE, "Page");
    static final ClassName BLOCK = ClassName.get(DATA_PACKAGE, "Block");
    static final ClassName VECTOR = ClassName.get(DATA_PACKAGE, "Vector");

    static final ClassName BIG_ARRAYS = ClassName.get("org.elasticsearch.common.util", "BigArrays");

    static final ClassName AGGREGATOR_STATE_VECTOR = ClassName.get(DATA_PACKAGE, "AggregatorStateVector");
    static final ClassName AGGREGATOR_STATE_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "AggregatorStateVector", "Builder");
    static final ClassName LONG_VECTOR = ClassName.get(DATA_PACKAGE, "LongVector");
    static final ClassName DOUBLE_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleVector");

    static final ClassName AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "AggregatorFunction");
    static final ClassName GROUPING_AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "GroupingAggregatorFunction");
}
