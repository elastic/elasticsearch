/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a class that implements an aggregation function with grouping.
 * See {@link Aggregator} for more information.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface GroupingAggregator {

    IntermediateState[] value() default {};

    /**
     * Exceptions thrown by the `combine*(...)` methods to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};

    /**
     * {@code true} if this is a time-series aggregation
     */
    boolean timeseries() default false;
}
